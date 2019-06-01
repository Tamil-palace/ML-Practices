import  requests

req=requests.get("http://pepitasuat.meritgroup.com")
with open("req.html","w") as fh:
    fh.write(str(req.content))
print(req.status_code)
