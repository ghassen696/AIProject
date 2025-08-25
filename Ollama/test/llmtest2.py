import ollama

response=ollama.list()
#print(response)

#chat exp
"""
res=ollama.chat(
    model="deepseek-r1:1.5b",
    messages=[
        {"role":"user","content":"whats 3+3 "}
    ]
)
print(res["message"]["content"])"""


mf = """
FROM deepseek-r1:1.5b
SYSTEM You are friday, a very smart assistant who categorizes the activities of the cloud team
into classes based on their input and classfying them with a descriptive description for use later for analysis and prediction
and monotoring and scoring and a report for each employee
PARAMETER temperature 0.1
"""

#now make the model
"""
ollama.create(model="Friday",from_="deepseek-r1:1.5b",system=You are friday, a very smart assistant who categorizes the activities of the cloud team
into classes based on their input and classfying them with a descriptive description for use later for analysis and prediction
and monotoring and scoring and a report for each employee)

#generate somrthing now

res=ollama.generate(model="Friday", prompt="cloud technologie")

print(res["response"])
"""
ollama.delete("friday")