# Postgres Table API

A robust FastAPI application designed to dynamically query, filter, and sort PostgreSQL tables. This API serves as a backend for data grids and administrative dashboards, providing flexible access to your database schema.

## Features

- **Dynamic Table Querying**: Fetch data from any table and schema.
- **Advanced Filtering**: Support for various operators based on data type (`eq`, `gt`, `lt`, `contains`, `starts_with`, etc.).
- **Sorting & Pagination**: Built-in support for server-side sorting and limiting results.
- **Helper Utilities**: Automatic column type detection and validation.
- **CORS Enabled**: Configured for local development integration (default: `http://localhost:3000`).

## Prerequisites

- **Python** 3.8+
- **PostgreSQL** database running locally or remotely.

## Installation

1. **Clone the repository** (if applicable) or navigate to the project directory.

2. **Create a virtual environment**:

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

The database connection string is currently hardcoded in `main.py` for demonstration purposes.

```python
# main.py
DATABASE_URL = "postgresql+asyncpg://user:password@localhost:5432/dbname"
```

> **Note**: For production, it is recommended to use environment variables (e.g., `python-dotenv`) to manage sensitive credentials.

## Running the Application

Start the development server using Uvicorn:

```bash
uvicorn main:app --reload
```

The API will be available at `http://127.0.0.1:8000`.

## API Documentation

Interactive API documentation (Swagger UI) is available at:

- **Swagger UI**: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
- **ReDoc**: [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc)

### Key Endpoint: Get Table Data

**`GET /table`**

| Parameter  | Type   | Default      | Description                             |
| ---------- | ------ | ------------ | --------------------------------------- |
| `table`    | string | **Required** | Name of the table to query.             |
| `schema`   | string | `public`     | Database schema name.                   |
| `limit`    | int    | `50`         | Number of records to return (max 5000). |
| `offset`   | int    | `0`          | Number of records to skip.              |
| `sort_by`  | string | `None`       | Column name to sort by.                 |
| `sort_dir` | string | `asc`        | Sort direction (`asc` or `desc`).       |
| `filters`  | json   | `None`       | JSON string of filters.                 |

#### Example Request

Fetching data from the `users` table where `age` is greater than 25:

```http
GET /table?table=users&limit=10&filters=[{"field":"age","op":"gt","value":25}]
```

#### Filter JSON Structure

The `filters` parameter expects a JSON array of objects:

```json
[
  {
    "field": "column_name",
    "op": "operator",
    "value": "search_value"
  }
]
```

**Supported Operators:**

- **String**: `eq`, `contains`, `starts_with`, `ends_with`
- **Number**: `eq`, `gt` (>), `gte` (>=), `lt` (<), `lte` (<=)
- **Boolean**: `eq`
- **Date/Datetime**: `eq`, `gt`, `gte`, `lt`, `lte`
