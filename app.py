from fastapi import FastAPI, Query, HTTPException
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError, RequestError, ConnectionError, TransportError


app = FastAPI()
es = Elasticsearch(hosts=["https://c9778aaeafc64b90a4d6ac93c7a07a46.eu-west-1.aws.found.io:443"], api_key="bG5NSGw1QUI4RmI3c1BVbWoyaUk6MHB2c2MycW9TTzZiOE9haUhIRk56dw==")
index_name = "locations"

@app.get("/suggest/")
async def suggest(query: str = Query(..., min_length=3)):
    
    try:
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