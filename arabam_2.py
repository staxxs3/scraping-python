import grequests, requests, math, sys, json, re, os
from datetime import datetime
from bs4 import BeautifulSoup
startTime = datetime.now()
_sockets = 10
start_link = 'https://www.arabam.com/ikinci-el?take=50&page='
page_link = 'https://www.arabam.com'
_results = []
data = datetime.now().date()
print(data)

with open("main_links.json", 'rb') as data_arabam:
    full_main_links = json.load(data_arabam)[50000:]


def scrap(_text, _url):
    car = {
        'Url': _url
    }
    body = BeautifulSoup(_text, 'html.parser')
    prix = body.select('.bcd-mid-extended > div > .color-red4')[0].text.strip()
    car['prix'] = prix.split(' ')[0].replace('.','')
    car['currency'] = prix.split(' ')[1]
    temp = body.select('.bcd-mid-extended > .one-line-overflow')[0].text.split('/')
    car['city'] = temp[0].strip()
    car['province'] = temp[1].strip()
    car['location'] = temp[2].strip()
    car['description'] = body.select('#js-hook-description')[0].text.strip()
    arr_info = body.select('.bcd-list-item')
    for row in arr_info:
        try:
            car[row.select('span')[0].text.replace(':','')] = row.select('span')[1].text
        except:
            do_nothing = 0
    # print(_url.split('/')[len(_url.split('/')) - 1])
    details = requests.get(
        'https://www.arabam.com/advertDetail/details?id=' + _url.split('/')[len(_url.split('/')) - 1])
    body_detail = BeautifulSoup(details.text, 'html.parser')
    det_arr = body_detail.select('div')
    temp_arr = {}

    for x in det_arr:
        if x.attrs['class'][0] == 'col-md-12':
            if not temp_arr == {}:
                car[temp_name.strip()] = temp_arr
            temp_name = x.select('p')[0].text
            temp_arr = {}
        else:
            inside = x.select('dl')
            for z in inside:
                temp_arr[z.select('dt')[0].text] = z.select('dd')[0].text
    car[temp_name] = temp_arr

    equipment = requests.get(
        'https://www.arabam.com/advertDetail/equipments?id=' + _url.split('/')[len(_url.split('/')) - 1])
    # print(equipment)
    body_detail = BeautifulSoup(equipment.text, 'html.parser')
    # print('x', body_detail, 'x')
    if not body_detail.text.strip() == '':
        # print(body_detail)
        eq_head = body_detail.select('h2')
        eq_ul = body_detail.select('ul')

        for x2 in range(len(eq_head)):
            temp_name = eq_head[x2].text.strip()
            temp_arr = []
            spans = eq_ul[x2].select('.one-line-overflow')
            for z in spans:
                # print(z.text)
                temp_arr.append(z.text.strip())
            car[temp_name] = temp_arr
    return car


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


new_arr = list(chunks(full_main_links, 5000))


# with open(str('./DATABASE/arabam/'+startTime.strftime('%Y-%m-%d')) + 'arabam_startlinks.json', 'w', encoding='utf8') as outfile:
#     json.dump(_main_links, outfile, indent=4, ensure_ascii=False)

# with open('./DATABASE/arabam/2019-09-09arabam_startlinks.json', encoding='utf-8') as file:
#     _main_links = json.load(file)[100000:]

print('##########################################################')
print('##', len(full_main_links), '-> number of taken links'.upper())
# main_links = unique(main_links)
print('##', len(full_main_links), '-> expected results after removing duplicates'.upper())
print('##########################################################')
licznik = 0
for _main_links in new_arr:
    licznik = licznik+1
    # print(_main_links)
    counter = 0
    for x in range(0, math.ceil(len(_main_links) / _sockets)):
        _intFrom = x * _sockets
        _intTo = 0
        if x < math.ceil(len(_main_links) / _sockets) - 1:
            _intTo = x * _sockets + _sockets
        else:
            _intTo = len(_main_links)

        rs2 = (grequests.get(_main_links[x]) for x in range(_intFrom, _intTo))

        for response2 in grequests.map(rs2):

            try:
                counter = counter + 1
                print('PART ' + str(licznik) + ':' + str(counter) + ':', response2.url)
                _results.append(scrap(response2.text, response2.url))
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)

    with open(str(startTime.strftime('%Y-%m-%d')) + 'arabam_part'+str(licznik)+'.json', 'w', encoding='utf8', errors='surrogatepass') as outfile:
        if outfile is not None:
            json.dump(_results, outfile, indent=4, skipkeys=True, ensure_ascii=False)
    _results = []