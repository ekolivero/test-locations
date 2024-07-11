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


@app.get("/suggest/")
async def suggest(query: str = Query(..., min_length=3)):
    
    index_name = "locations"
    
    try:
        
        q = {
            "query": {
                "match_phrase_prefix": {
                    "suggest": {
                        "query": query,
                        "max_expansions": 50
                    }
                }
            },
            "size": 20, 
            "sort": [{"sorting":{"order":"asc"}}, {"area": {"order":"desc"}}],
            "_source": ["id", "label", "level", "parents"] 
        }

        response = es.search(index=index_name, body=q)

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



@app.get("/geocoding")
async def geocoding(latitude: float = Query(..., description="Latitude of the point"),
                    longitude: float = Query(..., description="Longitude of the point")):
    
    index_name = "locations"

    try:
        q = {
            "_source": ['id', 'label', 'level', 'parents'],
            "sort": [
                {"level": {"order": "desc"}}
            ],
            "query": {
                "bool": {
                    "filter": {
                        "geo_shape": {
                            "geometry": {
                                "shape": {
                                "type": "point",
                                "coordinates": [longitude, latitude]
                                },
                            "relation": "intersects"
                            }
                        }
                    }
                }
            }
        }

        response = es.search(index=index_name, body=q)

        results = [r['_source'] for r in response['hits']['hits']]
        
        return {"results": results}

    except ElasticsearchException as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch error: {str(e)}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/houses/")
async def houses(level: str = Query(..., description="The level of the location hierarchy [region, province, city, district, neighborhood]"),
                 idx: str = Query(..., description="The id of the location")):

    index_name = 'houses'
    
    try:
        
        hierarchy_path = f"location.location.hierarchy.{level}.id"

        query = {
            "query": {
                "bool": {
                    "must": [
                        { "term": { hierarchy_path: idx }}
                    ]
                }
            }
        }

        response = es.search(index=index_name, body=query)

        return {"houses": [r['_source'] for r in response['hits']['hits']]}

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