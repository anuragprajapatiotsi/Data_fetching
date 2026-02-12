def _map_label_to_ui_type(label: str) -> str:
    l = label.lower()
    if "timestamp" in l: return "datetime"
    if "date" in l: return "date"
    if any(x in l for x in ["integer", "number", "decimal", "currency", "percentage"]): return "number"
    if "boolean" in l: return "boolean"
    return "string"

print(f"CalendarYear -> {_map_label_to_ui_type('CalendarYear')}")
print(f"Date -> {_map_label_to_ui_type('Date')}")
print(f"Integer -> {_map_label_to_ui_type('Integer')}")
print(f"String -> {_map_label_to_ui_type('String')}")
