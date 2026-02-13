# Semantic Modeling (Visual Dataset Designer) - Technical Design Document

## 1. System Overview

The Semantic Modeling feature allows developers and data analysts to visually design datasets by dragging and dropping tables, creating joins between them, and defining semantic types for columns. This "No-Code" interface empowers users to create complex queries without writing SQL. The system translates the visual model into a logical "Virtual Dataset" which can be queried dynamically.

**Key Concepts:**

- **Virtual Dataset / Logical Table**: A saved configuration of tables, joins, and column mappings that acts as a single queryable entity. The data is not materialized; instead, SQL is generated on-the-fly.
- **Semantic Type**: A user-friendly logical type (e.g., "Currency", "Percentage", "City") mapped to a physical column, driving UI formatting and filter behavior.

## 2. Architecture

The system follows a modern decoupled architecture:

```mermaid
graph TD
    User[User] --> Frontend[Next.js Frontend]
    Frontend -->|REST API| Backend[FastAPI Backend]
    Backend -->|SQLAlchemy| MetadataDB[(Metadata DB)]
    Backend -->|SQLAlchemy| DataWarehouse[(Data Warehouse)]

    subgraph "Frontend Layer"
        Frontend
        Store[Zustand Store / Context]
        Canvas[Infinite Canvas (dnd-kit)]
    end

    subgraph "Backend Layer"
        Backend
        Service[Semantic Service]
        QueryEngine[Query Service]
    end
```

### 2.1 Metadata vs. Actual Database

- **Metadata DB (PostgreSQL)**: Stores the _definition_ of datasets (tables, positions, joins, alias). It resides in the application's own database (e.g., `_prebuilt_sys` schema).
- **User Database (PostgreSQL)**: The actual data warehouse containing business tables. The system reads schema information (tables/columns) from here but treats it as read-only for modeling purposes.

### 2.2 Dynamic SQL Generation

The system does not create physical tables for datasets. Instead, the logical model is converted into a SQL query at runtime.
_Note: In the current implementation, the Frontend constructs the SQL string based on the visual graph and sends it to the Backend for secure execution._

## 3. Backend Design (FastAPI)

### 3.1 Entities (Data Models)

The backend uses SQLAlchemy models to persist the visual state of the modeler.

- **Dataset**: The root entity.
  - `id`: UUID (PK)
  - `name`: String
  - `description`: String
  - `created_at`: DateTime
- **DatasetTable**: A reference to a physical table included in the dataset.
  - `id`: UUID (PK)
  - `dataset_id`: UUID (FK)
  - `table_name`: String (Physical table name)
  - `alias`: String (Unique alias, e.g., `t1`, `users_1`)
  - `position_x`: Float (X-coordinate on canvas)
  - `position_y`: Float (Y-coordinate on canvas)

- **DatasetJoin**: Defines a relationship between two tables.
  - `id`: UUID (PK)
  - `dataset_id`: UUID (FK)
  - `left_table`: String (Alias of left table)
  - `left_column`: String (Column name)
  - `right_table`: String (Alias of right table)
  - `right_column`: String (Column name)
  - `join_type`: String (Enum: `inner`, `left`, `right`)

- **SemanticMapping**: (System Table `_prebuilt_sys.semantic_column_mapping`) Maps physical columns to semantic types.
  - `schema_name`: String
  - `table_name`: String
  - `column_name`: String
  - `sm_column_code`: String (e.g., `USD`, `PCT`, `DATE`)

### 3.2 API Endpoints

#### Dataset Management

- `GET /datasets`: List all available datasets.
- `POST /datasets`: Create a new empty dataset.
- `GET /datasets/{id}`: Retrieve full dataset definition (tables, joins).

#### Table & Join Operations

- `POST /datasets/{id}/tables`: Add a table to the canvas.
- `PATCH /datasets/{id}/tables/{table_id}`: Update position (drag & drop persistence).
- `POST /datasets/{id}/joins`: Create a join between two columns.
- `DELETE /datasets/{id}/joins/{join_id}`: Remove a join.

#### Semantic Type Metadata

- `POST /semantic/mapping`: Map a column to a semantic type.
- `GET /semantic/columns?schema=X&table=Y`: Get merged column metadata (physical types + semantic overrides).
- `GET /semantic/types`: List available semantic types (e.g., Currency vs Number).

#### Query Execution

- `POST /query`: Execute a SQL query.
  - **Payload**: `{ query: "SELECT ...", limit: 100, offset: 0 }`
  - **Behavior**:
    1.  Sanitizes SQL (checks for `DROP`, `DELETE`).
    2.  Executes `COUNT(*)` for pagination.
    3.  Executes data query with `LIMIT/OFFSET`.
    4.  Enriches result headers with semantic types (detects source table from FROM clause).
    5.  Returns results + query statistics.

## 4. Frontend Design (Next.js)

The frontend uses **React**, **Tailwind CSS**, and **dnd-kit** to provide a rich interactive experience.

### 4.1 Major Components

- **SemanticModeler**: Top-level page component using `ReactFlow` or `dnd-kit` context.
  - Manages the `SemanticContext` provider.
  - Divides screen into Sidebar (Schema Explorer) and Canvas.
- **SchemaExplorer**: Tree view of database schemas and tables.
  - Draggable source (`useDraggable`).
- **Canvas (SemanticCanvas)**: Infinite drop zone (`useDroppable`).
  - Renders `SemanticCard`s based on `dataset.tables`.
- **SemanticCard**: Node representing a table.
  - Displays list of columns.
  - **Interaction**: Clicking a column initiates "Join Mode".
- **JoinLines**: SVG Layer overlay.
  - Renders Bezier curves connecting joined columns.
  - Calculates start/end coordinates based on `SemanticCard` positions and column index.
- **JoinDialog**: Modal triggered when completing a join connection.
  - Allows selecting Join Type (Inner/Left/Right).

### 4.2 State Management

The `SemanticContext` (React Context/Zustand) is the source of truth for the session:

- `tables`: Array of table nodes with (x, y).
- `joins`: Array of connections.
- `pendingJoinSource`: `{ tableId, columnName } | null`. Tracks the start of a join drag/click.
- `cachedColumns`: Dictionary of table metadata to prevent redundant fetch requests.

## 5. Data Flow

### A. Drag Table to Canvas

1.  **Action**: User drags "Users" table from Sidebar to Canvas.
2.  **Frontend**: `onDragEnd` detects drop. Calculates (x, y) relative to canvas origin.
3.  **API**: `POST /datasets/{id}/tables` with `{ table_name: "users", position_x: 100, position_y: 200 }`.
4.  **Backend**: Creates `DatasetTable` record. Returns ID.
5.  **UI**: Updates `tables` state; `SemanticCard` appears.

### B. Move Table & Persist

1.  **Action**: User moves "Users" card.
2.  **Frontend**: Updates internal state immediately (optimistic UI). Join lines redraw in real-time.
3.  **API**: On drop, `PATCH /datasets/{id}/tables/{table_id}` with new coordinates.
4.  **Backend**: Updates database record.

### C. Create Join

1.  **Action**: User clicks "id" on "Users" table.
    - _State Update_: `pendingJoinSource = { table: "users", column: "id" }`.
    - _UI_: Highlights "id" column.
2.  **Action**: User clicks "user_id" on "Orders" table.
3.  **Frontend**: Detects valid target. Opens `JoinDialog`.
4.  **Action**: User selects "Left Join" and confirms.
5.  **API**: `POST /datasets/{id}/joins` with source/target tables and columns.
6.  **Backend**: Validates tables belong to dataset. Saves `DatasetJoin`.
7.  **UI**: Adds join to state. `JoinLines` component draws the line.

### D. Run Dataset Query

1.  **Action**: User clicks "Run Query".
2.  **Frontend Logic**:
    - Collects all `dataset.tables`.
    - Collects all `dataset.joins`.
    - Constructs SQL:
      ```sql
      SELECT t1.*, t2.*
      FROM users t1
      LEFT JOIN orders t2 ON t1.id = t2.user_id
      ```
3.  **API**: `POST /query` with the generated SQL.
4.  **Backend**: Executes safely.
5.  **UI**: Renders `AdvancedDataTable` with results.

## 6. Query Execution logic

The backend `query_service` handles the execution safely:

1.  **Safety Check**: Regex scan for destructive keywords (`DROP`, `ALTER`, `GRANT`).
2.  **Pagination**: Wraps the user query in a `SELECT * FROM (...) LIMIT :limit OFFSET :offset`.
3.  **Counting**: Runs a separate `SELECT COUNT(*)` on the wrapped query to support pagination UI ("Page 1 of 50").
4.  **Semantic Enrichment**:
    - Parses the `FROM` clause to identify the primary table.
    - Fetches semantic mappings for that table (e.g., `amount` -> `Currency`).
    - Returns data with schema `{ key: "amount", type: "currency" }` so the frontend knows to format it as `$1,234.56`.

## 7. Error Handling

- **Invalid Joins**: Frontend prevents joining a table to itself without aliasing (future feature). API rejects joins for tables not in the dataset.
- **Disconnected Graph**: The UI should visually warn if tables are isolated (not connected by lines).
- **Backend Validation**:
  - `404 Not Found`: If dataset or table ID is invalid.
  - `400 Bad Request`: If SQL contains syntax errors or restricted keywords.
  - `500 Internal Server Error`: For DB connection issues (e.g., handled by global exception handler).

## 8. Future Improvements

- **Materialized Views**: Allow saving the dataset query as a persistent View in the database for performance.
- **Backend Query Builder**: Migrate SQL generation logic to the backend (`GET /datasets/{id}/sql`) to centralize complex join logic and database-specific optimizations.
- **Caching**: Implement Redis caching for query results based on a hash of the query string.
- **Permissioning**: Add Row-Level Security (RLS) or dataset sharing permissions (Public/Private).
