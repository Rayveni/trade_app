from lxml import html
from sys import path
path.append('/worker/common_libs')
from selenium_wrapper import selenium_wrapper
from json import load
from datetime import datetime
from base64 import b64encode

finam_urls={'moex_sec_dividends':r'https://www.finam.ru/quote/moex/{}/dividends/'}

with open('/worker/worker/src/jobs/selenium_config.json', 'r') as file:
    selenium_config = load(file)

def finam_divs(secid:str)->dict:
    sl=selenium_wrapper(selenium_config)
    scrap_result=sl.get_page_source(finam_urls['moex_sec_dividends'].format(secid.lower()))
    if scrap_result['success']==False:
        return scrap_result
    original_page=scrap_result['page_source']
    extract_divs=__extract_divs(original_page,secid)
    extract_divs['html']=b64encode(original_page.encode('utf-8')) #base64.b64decode(data[-2][1]).decode('utf-8')
    return extract_divs

def __extract_divs(page_txt:str,secid:str)->dict:
    #_html=b64decode(byte_html['dividends_html']).decode('utf-8')  
    root = html.document_fromstring(page_txt) 
    page_title_search = root.xpath('//title/text()') 
    if page_title_search==[]:
        res={'success':False,'error_message':'empty page_title_search'}
    else:
        page_title=page_title_search[0]
        if page_title[:3]=='404':
             res={'success':False,'error_message':f'404 code page_title: {page_title}'}
        else:         
            _table=root.xpath("//table[contains(@class, 'finfin-local-plugin-instrument-item-part-dividends-table')]")[0]
            header_row,parsed_rows=[],[]
            for _row in _table.xpath('.//tr'):
                parsed_row=_row.xpath("td//text()")
                if len(parsed_row)==0:
                    header_row=_row.xpath(".//th//text()")
                else:
                    parsed_rows.append(_convert_finam_divs(secid,parsed_row)) 
            res={'success':True,'divs':parsed_rows}  
    return res  


def _convert_finam_divs(secid:str,_div_row:list)->list:
    _finam_date_format,pg_date_format='%d.%m.%Y','%Y-%m-%d'
    if len(_div_row)>3:
        _rate=_div_row[3].replace(',','.').replace('%','')
        _rate=float(_rate)
    else:
        _rate='None'
    return [secid,
            #datetime.strptime(_div_row[0], _finam_date_format).strftime(pg_date_format) ,
            datetime.strptime(_div_row[1], _finam_date_format).strftime(pg_date_format),
            float(_div_row[2].split('\xa0')[0].replace(',','.')),str(_rate)
            
        
    ]