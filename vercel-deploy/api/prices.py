import json
from pathlib import Path

DATA_FILE = Path(__file__).parent.parent / "data.json"

def load_data():
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"latest": None, "history": [], "settings": {}, "logs": [], "alerts": [], "summary": {}}

def GET(request):
    """Vercel Python Serverless Function - GET handler"""
    data = load_data()
    path = request.path
    
    if path == "/api/prices/latest":
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"item": data["latest"], "summary": data["summary"]}, ensure_ascii=False)
        }
    
    if path.startswith("/api/prices/history"):
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"items": data["history"][:25], "total": len(data["history"]), "summary": data["summary"]}, ensure_ascii=False)
        }
    
    if path == "/api/settings":
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(data["settings"], ensure_ascii=False)
        }
    
    if path == "/api/jobs/logs":
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"items": data["logs"]}, ensure_ascii=False)
        }
    
    if path == "/api/alerts":
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"items": data["alerts"]}, ensure_ascii=False)
        }
    
    return {
        "statusCode": 404,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"error": "Not found"}, ensure_ascii=False)
    }
