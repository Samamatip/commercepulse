from pathlib import Path
import json
from typing import List, Dict, Any


def load_json_file(file_path: Path) -> List[Dict[str, Any]]:
    
    with open(file_path, 'r', encoding='utf-8') as f:
        # Try to load as JSON array first
        try:
            data = json.load(f)
            if isinstance(data, list):
                return data
            else:
                return [data]
        except json.JSONDecodeError:
            # If that fails, try JSONL format (one JSON object per line)
            f.seek(0)
            records = []
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
            return records
    return []  # Return empty list if no data found
