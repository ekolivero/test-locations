from fastapi import FastAPI, Query, HTTPException
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError, RequestError, ConnectionError, TransportError
import os
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

#es = Elasticsearch(hosts=["http://localhost:9200"])
es = Elasticsearch(hosts=[os.getenv('ELASTIC_HOST')], api_key=os.getenv('ELASTIC_API_KEY'))
print('Elastic ping:', es.ping())
index_name = "locations"

@app.get("/suggest/")
async def suggest(query: str = Query(..., min_length=3)):

    try:
        
        """
        query = {
            "suggest": {
                "suggestions": {
                    "prefix": query,
                    "completion": {
                        "field": "suggest",
                        "size": 10 
                    }
                }
            }
        }

        response = es.search(index=index_name, body=query)

        suggestions = []
        if "suggest" in response:
            options = response["suggest"]["suggestions"][0]["options"]
            suggestions = [opt["_source"]["label"] for opt in options]
        """

        query = {
            "query": {
                "multi_match": {
                    "query": query,
                    "type": "bool_prefix",    
                    "fields": ["suggest"]
                }
            },
            "size": 20,
            "sort": [{"sorting":{"order":"asc"}}, {"area": {"order":"desc"}}],
            "_source": ["id", "label","level","parents"]
        }

        response = es.search(index=index_name, body=query)

        suggestions = []
        for r in response['hits']['hits']:
            res = {
                'id': r['_source']['id'],
                'label': r['_source']['label'],
                'level': r['_source']['level'],
                'parents': r['_source']['parents'][0:-1]
            }
            suggestions.append(res)

        return {"suggestions": suggestions}

    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Resource not found")

    except RequestError as e:
        raise HTTPException(status_code=400, detail="Bad request")

    except ConnectionError as e:
        raise HTTPException(status_code=503, detail="Elasticsearch connection error")

    except TransportError as e:
        raise HTTPException(status_code=503, detail="Elasticsearch transport error")

    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")




if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)