import argparse
import json
import sys
from jsonschema import Draft202012Validator

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--payload", required=True)
    ap.add_argument("--schema", required=True)
    args = ap.parse_args()

    with open(args.payload, "r", encoding="utf-8") as f:
        payload = json.load(f)

    with open(args.schema, "r", encoding="utf-8") as f:
        schema = json.load(f)

    v = Draft202012Validator(schema)
    errors = sorted(v.iter_errors(payload), key=lambda e: e.path)

    if errors:
        print("Payload schema validation FAILED:\n")
        for e in errors:
            path = ".".join([str(p) for p in e.path]) or "(root)"
            print(f"- {path}: {e.message}")
        return 2

    print("Payload schema validation OK.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
