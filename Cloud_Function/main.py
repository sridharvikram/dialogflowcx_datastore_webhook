import json
import logging
import functions_framework
import requests
import logging
import re
import time

from google.cloud import discoveryengine_v1alpha as dengine

PROJECT_ID = "your project id"
LOCATION_ID = "datastore location id"
DATASTORE = "your datastore id"



client = dengine.SearchServiceClient()

def datastore(data: dict):
    model=data['model']
    serving_config = client.serving_config_path(
        project=PROJECT_ID,
        location=LOCATION_ID,
        data_store=DATASTORE,
        serving_config="default_config")

    content_search_spec = {
        "summary_spec": {
              "summary_result_count": 2,
              "ignore_adversarial_query": False,
              "ignore_non_summary_seeking_query": False,
              "model_prompt_spec": {
                  "preamble": "If there is no information to create the summary, then say: No Match"
                  }
            },
        "snippet_spec": {
            "return_snippet": True
        },
        "extractive_content_spec": {
            "max_extractive_answer_count": 1,
            "max_extractive_segment_count": 1,
            },
        }

    request = dengine.SearchRequest(
        serving_config=serving_config,
        query=data['query'],
        query_expansion_spec={ "condition": "AUTO" },
        spell_correction_spec={ "mode": "AUTO" },
        filter=f"model: ANY(\"{model}\")",
        content_search_spec=content_search_spec
        )

    response = client.search(request)
    #print("response from ds",response)
    return response

def decode_datastore_payload(response):

    if len(response.results) == 0 or not response.results[0].document.derived_struct_data.get("extractive_answers"):
        return {
            "link": None,
            "extractive_answers": [{"content": "Results not found"}],
            "summary": "Results not found"
        }

    if response.summary.summary_text == "No Match":
        return {
            "link": None,
            "extractive_answers": [{"content": "Results not found"}],
            "summary": "Results not found"
        }

    link = response.results[0].document.derived_struct_data.get("link")
    extractive_answers = []

    for r in response.results[0].document.derived_struct_data.get("extractive_answers"):
        item = {
            "content": r.get("content"),
            "pageNumber": r.get("pageNumber")
        }
        extractive_answers.append(item)
    
    summary = response.summary.summary_text

    return {
        "link": link,
        "extractive_answers": extractive_answers,
        "summary": summary
    }

@functions_framework.http
def hello_http(request):
    start = time.perf_counter()

    data = request.get_json()

    raw_resp = datastore(data)
    #print(raw_resp)

    resp=decode_datastore_payload(raw_resp)
    end = time.perf_counter() - start
    print('{:.6f}s Latency'.format(end))
    print("response",resp)
    return resp
