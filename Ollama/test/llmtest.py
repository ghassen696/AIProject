import requests
import json

url= "https:localhost:11434/api/generate"

data={

    "model":"deepseek-r1:1.5b",
    "prompt":"hey",
}
response= requests.post(url,json=data, stream=True)

if response.status_code==200:
    print("generated text:",end="", flush=True)
    for line in response.iter_lines():
        if line:
            decoder_line=line.decoder("utf-8")
            result= json.load(decoder_line)
            generated_text= result.get("response","")
            print(generated_text,end="",flush=True)
else:
    print("Error", response.status_code,response.text)