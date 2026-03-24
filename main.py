import time
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field
from services.transform_service import transform_json
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI(title="FluxData API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve UI files from the project root
_base_dir = os.path.dirname(os.path.abspath(__file__))

@app.get("/")
async def serve_ui():
    return FileResponse(os.path.join(_base_dir, "index.html"))

from typing import Any

class TransformRequest(BaseModel):
    model_config = ConfigDict(extra='allow')
    source: Any
    target_schema: dict = Field(alias="schema")

@app.post("/api/v1/transform")
async def transform_endpoint(request: TransformRequest):
    start_time = time.time()
    try:
        # Call the transformation service
        result = await transform_json(request.source, request.target_schema)
        
        latency_ms = int((time.time() - start_time) * 1000)
        
        return {
            "success": True,
            "transformed": result.get("transformed", {}),
            "meta": {
                "fields_mapped": result.get("fields_mapped", 0),
                "fields_nulled": result.get("fields_nulled", 0),
                "latency_ms": latency_ms
            }
        }
    except Exception as e:
        # We handle failures gracefully as requested
        return {
            "success": False,
            "error": "parse_failed" if "parse" in str(e).lower() else "transformation_failed",
            "message": str(e) or "AI output could not be parsed. Retry or simplify source JSON.",
            "retry_suggested": True
        }
