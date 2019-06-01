from incapsula import IncapSession
import re
import requests
from datetime import datetime
import sys,os

session = IncapSession()
url ='https://kleinpod.pilbaraports.com.au/dashb.ashx?db=audam.dailyshipping'
#url = 'https://tis.eustream.sk/TisWeb/services/JSONPublicService/getPublicFlows?response=application/json&_=1515405720034&gasDate=20180108&isWatt=true&language=EN'
#url = 'http://www.simplybe.co.uk/shop/pack-of-two-pull-on-shorts/af111/product/details/show.action?pdBoUid=5205#colour:Black/White,size:'
#headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36','Host':'tis.eustream.sk','Referer':'https://tis.eustream.sk/TisWeb/'}
#cookie='JSESSIONID=8893CE6FA787FFAF6317339D60F0D53D.tomcat1DCS105; BNES_JSESSIONID=UVfLIleSZ4KlMmR3jR9E04QmWgezhRb9iahoS5ircKcZ1dgzWPiiQbe1lI8L/F1SHYFwYJ6uKWy13jwsNBMafYMpsUt1/gWgEx8buOItov2hMPhhG99inakMpIm0RY8z/Gb8IqeF57c=; BNI_persistence=0000000000000000000000006902c80a00008223'
#headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'}
#response=session.get(url,headers=headers)
response=session.get(url)
#response = requests.get(url)
content=response.content
print(content)