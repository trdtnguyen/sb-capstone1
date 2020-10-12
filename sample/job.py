import requests
import json
import prettytable
headers = {'Content-type': 'application/json'}

series_ids = ['CUUR0000SA0','SUUR0000SA0']
start_year = "2019"
end_year = "2020"
API_url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
data = json.dumps({"seriesid": series_ids,"startyear":start_year, "endyear":end_year})

p = requests.post(API_url, data=data, headers=headers)
json_data = json.loads(p.text)
for series in json_data['Results']['series']:
    x=prettytable.PrettyTable(["series id","year","period","value","footnotes"])
    seriesId = series['seriesID']
    for item in series['data']:
        year = item['year']
        period = item['period']
        value = item['value']
        footnotes=""
        for footnote in item['footnotes']:
            if footnote:
                footnotes = footnotes + footnote['text'] + ','
        if 'M01' <= period <= 'M12':
            x.add_row([seriesId,year,period,value,footnotes[0:-1]])
    output = open(seriesId + '.txt','w')
    output.write (x.get_string())
    output.close()