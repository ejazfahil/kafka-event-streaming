"""Schema registry 2024-08-19"""
import requests,json
class SchemaRegistry:
    def __init__(self,url="http://localhost:8081"): self.url=url
    def get_schema(self,subject,version="latest"):
        r=requests.get(f"{self.url}/subjects/{subject}/versions/{version}",timeout=5)
        r.raise_for_status(); return json.loads(r.json()["schema"])
    def register_schema(self,subject,schema:dict):
        payload={"schema":json.dumps(schema)}
        r=requests.post(f"{self.url}/subjects/{subject}/versions",json=payload,timeout=5)
        r.raise_for_status(); return r.json()["id"]
