
import re
import io
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
import PyPDF2


HEADERS = {'User-Agent':('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                         'AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/73.0.3683.86 Safari/537.36')}

ADAMS_QUERY_STRING = (
"https://adams.nrc.gov/wba/services/search/advanced/nrc?q=("
        "mode:sections,sections:("
                "filters:("
                        "public-library:!t),"
                        "properties_search_all:!("
                                "!(PublishDatePARS,ge,'{start_date}+12:00+AM',''),"
                                "!(PublishDatePARS,le,'{end_date}+12:00+AM','')"
                ")"
        ")"
")&qn=New&tab=advanced-search-pars&s=DocumentDate&so=ASC"  
)

ADAMS_API_FIELDS = ['accessionnumber', 'addresseeaffiliation', 'addresseename',
       'authoraffiliation', 'authorname', 'casereferencenumber',
       'compounddocumentstate', 'contentsize', 'datedocketed', 'docketnumber',
       'documentdate', 'documentreportnumber', 'documenttitle', 'documenttype',
       'estimatedpagecount', 'keyword', 'licensenumber', 'mimetype',
       'packagenumber', 'publishdatepars', 'uri']

EN_UNIT_FIELDS = ['Current PWR 1', 'Current PWR 2', 'Current PWR 3', 'Current RX Mode 1',
               'Current RX Mode 2', 'Current RX Mode 3', 'Initial PWR 1', 'Initial PWR 2',
               'Initial PWR 3', 'Initial RX Mode1', 'Initial RX Mode 2', 'Initial RX Mode 3',
               'RX CRIT 1', 'RX CRIT 2', 'RX CRIT 3', 'Scram Code 1', 'Scram Code 2',
               'Scram Code 3']

EN_CFRS = ['20.1906(d)(1) - SURFACE CONTAM LEVELS > LIMITS',
       '20.1906(d)(2) - EXTERNAL RAD LEVELS > LIMITS',
       '20.2201(a)(1)(i) - LOST/STOLEN LNM>1000X',
       '20.2201(a)(1)(ii) - LOST/STOLEN LNM>10X',
       '20.2202(a)(1) - PERS OVEREXPOSURE/TEDE >= 25 REM',
       '20.2202(a)(2) - EXCESSIVE RELEASE',
       '20.2202(b)(1) - PERS OVEREXPOSURE/TEDE >= 5 REM',
       '20.2202(b)(2) - EXCESSIVE RELEASE',
       '21.21 - UNSPECIFIED PARAGRAPH',
       '21.21(a)(2) - INTERIM EVAL OF DEVIATION',
       '21.21(d)(3)(i) - DEFECTS AND NONCOMPLIANCE',
       '26.417(b)(1) - FFD PROGRAMATIC FAILURE',
       '26.417(b)(1) - FFD PROGRAMMATIC FAILURE',
       '26.719 - FITNESS FOR DUTY', '26.73 - FITNESS FOR DUTY',
       '30.50(a) - PROTECTIVE ACTION PREVENTED',
       '30.50(b)(1) - UNPLANNED CONTAMINATION',
       '30.50(b)(2) - SAFETY EQUIPMENT FAILURE',
       '30.50(b)(3) - MED TREAT INVOLVING CONTAM',
       '30.50(b)(4) - FIRE/EXPLOSION',
       '35.3045(a)(1) - DOSE <> PRESCRIBED DOSAGE',
       '35.3045(a)(2) - DOSE > SPECIFIED EFF LIMITS',
       '35.3045(a)(3) - DOSE TO OTHER SITE > SPECIFIED LIMITS',
       '35.3045(b) - PATIENT INTERVENTION DAMAGE',
       '35.3047(a) - EMBRYO/FETUS DOSE  > 50 mSv',
       '36.83(a)(1) - UNSHIELD STUCK SOURCE',
       '36.83(a)(10) - POOL COND HIGH',
       '36.83(a)(4) - FAILED CABLE/DRIVE',
       '36.83(a)(5) - INOP ACCESS CTRL SYS',
       '37.57(a) - ACT/ATTEMPT THEFT CAT 1/2 RAD MATL',
       '37.81(b) - LOST/MISSING CAT 2 RAD MATL IN TRANSIT',
       '40.35(f) - EMERGENCY DECLARED',
       '40.60(b)(1) - UNPLANNED CONTAMINATION',
       '40.60(b)(2) - SAFETY EQUIPMENT FAILURE',
       '40.60(b)(3) - MED TREAT INVOLVING CONTAM',
       '40.60(b)(4) - FIRE/EXPLOSION', '50.55(e) - CONSTRUCT DEFICIENCY',
       '50.72(a) (1) (i) - EMERGENCY DECLARED',
       '50.72(b)(2)(xi) - OFFSITE NOTIFICATION',
       '50.72(b)(1) - DEVIATION FROM T SPEC',
       '50.72(b)(2)(i) - PLANT S/D REQD BY TS',
       '50.72(b)(3)(ii)(A) - DEGRADED CONDITION',
       '50.72(b)(3)(v)(D) - ACCIDENT MITIGATION',
       '50.72(b)(2)(iv)(A) - ECCS INJECTION',
       '50.72(b)(2)(iv)(B) - RPS ACTUATION - CRITICAL',
       '50.72(b)(3)(iv)(A) - VALID SPECIF SYS ACTUATION',
       '50.72(b)(3)(xii) - OFFSITE MEDICAL',
       '50.72(b)(3)(ii)(B) - UNANALYZED CONDITION',
       '50.72(b)(3)(v)(C) - POT UNCNTRL RAD REL',
       '50.72(b)(3)(v)(B) - POT RHR INOP',
       '50.72(b)(3)(xiii) - LOSS COMM/ASMT/RESPONSE',
       '50.72(b)(3)(v)(A) - POT UNABLE TO SAFE SD',
       '50.73(a)(1) - INVALID SPECIF SYSTEM ACTUATION',
       '70.32(i) - EMERGENCY DECLARED',
       '70.50(a) - PROTECTIVE ACTION PREVENTED',
       '70.50(b)(1) - UNPLANNED CONTAMINATION',
       '70.50(b)(2) - SAFETY EQUIPMENT FAILURE',
       '70.50(b)(3) - MED TREAT INVOLVING CONTAM',
       'PART 70 APP A (c) - OFFSITE NOTIFICATION/NEWS REL',
       '70.50(b)(4) - FIRE/EXPLOSION',
       '70.74 APP. A - ADDITIONAL REPORTING REQUIREMENTS',
       '72.74 - CRIT LOSS/THEFT OF SNM',
       '72.75(b)(2) - PRESS RELEASE/OFFSITE NOTIFICATION',
       '72.75(c )(1) - SPENT FUEL, HLW, RX GTCC DEFECT',
       '72.75(c )(2) - SPENT FUEL, HLW OR RX-REL GTCC RED. EFECT',
       '72.75(d)(1) - SFTY EQUIP. DISABLED OR FAILS TO FUNCTION',
       '73.71(b)(1) - SAFEGUARDS REPORTS', '74.11(a) - LOST/STOLEN SNM',
       '74.57 - ALARM RESOLUTION', '76.120(a)(4) - EMERGENCY DECLARED',
       '76.120(c)(1) - UNPLANNED CONTAMINATION',
       '76.120(c)(2) - SAFETY EQUIPMENT FAILURE', 'AGREEMENT STATE',
       'INFORMATION ONLY', 'NON-POWER REACTOR EVENT',
       'OTHER UNSPEC REQMNT',
       'PART 70 APP A (a)(4) - ALL SAFETY ITEMS UNAVAILABLE',
       'PART 70 APP A (a)(5) - ONLY ONE SAFETY ITEM AVAILABLE',
       'PART 70 APP A (b)(1) - UNANALYZED CONDITION',
       'PART 70 APP A (b)(2) - LOSS OR DEGRADED SAFETY ITEMS',
       'PART 70 APP A (b)(3) - ACUTE CHEMICAL EXPOSURE',
       'PART 70 APP A (b)(4) - NAT PHENOM AFFECTING SAFETY',
       'PART 70 APP A (b)(5) - DEV FROM ISA',
       'RESEARCH AND TEST REACTOR EVENT', 'RESPONSE-BULLETIN']

enhref = re.compile(r'#en\d+')
enRegex = re.compile(r'(.*)#(en\d{5})$')
enTagRegex = re.compile(r'<a\sname="en(\d{5})"><\/a>')
internalENTagRegex = re.compile(r'<a\sname="en(\d{5})">')
adamsPackageRE = re.compile(r'ML\w{9}\.html$')
phonePattern = re.compile(r'''(
    (\d{3}|\(\d{3}\))?                # area code
    (\s|-|\.)?                        # separator
    (\d{3})                           # first 3 digits
    (\s|-|\.)                         # separator
    (\d{4})                           # last 4 digits
    (\s*(ext|x|ext.)\s*(\d{2,5}))?    # extension
    )''', re.VERBOSE)

htmlRegex = re.compile('<!-- #BeginEditable "Page Content" -->(.*?)<!--', re.DOTALL)

SCRAM_CODES = ['A/R', 'M/R', 'A', 'N', 'M']
REACTOR_CRIT_CODES = ['Y', 'N']
REACTOR_MODE_CODES = ['Cold Shutdown', 'Decommissioned', 'Defueled',
                      'Hot Shutdown', 'Hot Standby', 'Intermediate Shutdown',
                       'Power Operation', 'Refueling', 'Refueling Shutdown',
                       'Startup', 'Under Construction']

def soupify(url):
    with requests.Session() as s:
        html = s.get(url).text
    return BeautifulSoup(html, 'lxml')


def squarify_table(tableSoup):
    header = [td.text for td in tableSoup.find('tr').findAll('td')]
    soup = tableSoup.parent.parent.parent
    for tr in tableSoup.findAll('tr'):
        if len(tr.findAll('td')) < len(header):
            td = soup.new_tag('td', align='center')
            tr.append(td)


def parse_en(text):
    text = text.replace('"Less than Cat 3\n', '"Less than Cat 3')
    text = text.replace('"Category 2\n', '"Category 2"')
    text = text.replace('"Category 3\n', '"Category 3"')
    text = text.replace('"Category 1\n', '"Category 1"')
    text = text.replace('(NRC()', '(NRC)')
    enData = {}
    lines = [l.strip().replace('\xa0', '') 
             for l in text.split('\n') if l.strip()]
    
    # Group EN Text. This is everything after 'Event Text' or Event Text *** NOT FOR PUBLIC DISTRIBUTION ***
    textStart = [index for index, line in enumerate(lines) if line.lower().startswith('event text')][0]
    text = '\n'.join(lines[textStart + 1:])
    enData['Event Text'] = re.sub('Page Last Reviewed/Updated.*', '', text)
    lines = lines[:textStart]
    
    # Fix state
    for index, line in enumerate(lines):
        if 'State: ' in line:
            state = re.search(r'State:\s(\w+)', line)
            lines[index] = re.sub(r'State:\s(\w+)', '', line)
            enData['State'] = state.group(1) if state else 'State: '
            break
    # CHANGE COMMENTS
    comments = [index for index, line in enumerate(lines) if 'Comments:' in line]
    comments_ = ''
    if comments:
        comments_ += lines[comments[0]]
        nextLine = comments[0] + 1
        while ':' not in lines[nextLine]:
            comments_ += '\n' + lines[nextLine]
            nextLine += 1
        # Delete comments lines
        comment_lines = lines[comments[0]: nextLine]
        for cline in comment_lines:
            lines.remove(cline)
        enData['Comments'] = comments_.replace('Comments: ', '')
    retraction = False
    retractionText = ['!!!!! THIS EVENT HAS BEEN RETRACTED. THIS EVENT HAS BEEN RETRACTED  !!!!!',
                      '!!!!! THIS EVENT HAS BEEN RETRACTED.  THIS EVENT HAS BEEN RETRACTED !!!!!',
                      '!!!!! THIS EVENT HAS BEEN RETRACTED.THIS EVENT HAS BEEN RETRACTED !!!!!']

    cat3RE = re.compile(r'This material event contains a \"(.*)\" level of radioactive material\.', flags=re.DOTALL)
    
    for retraction_ in retractionText:
        if retraction_ in lines:
            retraction = True
            lines.remove(retraction_)
            break
    enData['Retraction'] = retraction
    enData['Event Type'] = lines[0]   
    cfrStart = [index for index, line in enumerate(lines) if line.lower() == '10 cfr section:' ][0] + 1
    cfrEnd = [index for index, line in enumerate(lines) if line == 'Person (Organization):' or line == 'Unit'][0]
    enData['10 CFR Section'] = lines[cfrStart:cfrEnd]
    try:
        lines.remove('10 CFR Section:')
    except ValueError:
        lines.remove('10 CFR SECTION:')
    
    personsStart = cfrEnd
    personEnd = textStart - 1
    cat3 = [line for line in lines if cat3RE.search(line)]
    if cat3:
        enData['Material Category'] = cat3RE.search(cat3[0]).group(1).replace('"', '')
        personEnd -= 1
        lines.remove(cat3[0])

    if enData['Event Type'] == 'Power Reactor':
        personEnd = [index for index, line in enumerate(lines) if line == 'Unit'][0]

    enData['Person (Organization)'] = lines[personsStart:personEnd]
    try:
        lines.remove('Person (Organization):')
    except ValueError:
        lines.remove('PERSON          ORGANIZATION')
    
    pairsRegex = re.compile(r'^([\w\s#]+):(\s([\w\s\[\]\-\/:]+))?')
    pairsRegex = re.compile(r'^([\w\s#]+):(\s([\.\,\w\s\[\]\-\/:]+))?')
    
    for l in lines:
        m = pairsRegex.search(l)
        if m:
            key = m.group(1).replace('SCAM', 'SCRAM').replace('RX Crit', 'RX CRIT')
            if m.group(2):
                value = m.group(2)
            else:
                value = ''
            enData[key] = value.strip()
    return enData


def parse_unit(unit):
    if pd.isnull(unit):
        return [pd.np.nan] * 3
    units = re.findall(r'\[(\d?)\]', unit)
    units = [float(u) if u else pd.np.nan for u in units]
    return units


def parse_persons(people):
    # Return list of [staff name, org name] * 10
    result = []
    for p in people:
        staff, org = re.search(r'(.*)(\(.*\))$', p).groups()
        staff = staff.strip()
        org = org.replace(')', '').replace('(', '').strip()
        result.append(staff)
        result.append(org)
    result = result + [pd.np.nan] * (20 - len(result))
    return result


def parse_unit_data(unit_data):
    if pd.isnull(unit_data):
        return [pd.np.nan] * len(EN_UNIT_FIELDS)
    data = []
    for key in unit_data:
        unit_data[key] = list(unit_data[key]) + [pd.np.nan] * (3 - len(unit_data[key]))
    data.extend(unit_data['Current PWR'])
    data.extend(unit_data['Current RX Mode'])
    data.extend(unit_data['Initial PWR'])
    data.extend(unit_data['Initial RX Mode'])
    try:
        data.extend(unit_data['RX CRIT'])
    except:
        data.extend(unit_data['RX Crit'])
    try:
        data.extend(unit_data['SCRAM Code'])
    except:
        data.extend(unit_data['SCAM Code'])
    return data


def fix_cfr(cfr):
    cfr = ''.join(cfr)
    cfr = [c for c in EN_CFRS if c in cfr]
    return cfr


def parse_site_name(row):
    site_name = row.get('Rep Org')
    site_name = pd.np.nan if site_name is None else site_name
    facility = row.get('Facility')
    facility = pd.np.nan if facility is None else facility
    if pd.isnull(site_name) and pd.isnull(facility):
        raise AttributeError
    return site_name if pd.notnull(site_name) else facility
    

class HTMLPage(object):
    def __init__(self, url):
        self.url = url
        self.online = True if self.url.startswith('http') else False
        self.status_code = None
        self._html = self._get_html()
        self.soup = self.soupify()
        self.title = self.soup.title if self.soup.title else ''
    
    
    def _get_html(self):
        if self.online:
            with requests.Session() as s:
                r = s.get(self.url, headers=HEADERS)
                self.status_code = r.status_code
                html = r.text
        else:
            with open(self.url, encoding='latin') as file:
                html = file.read()
        return html


    def soupify(self):
        return BeautifulSoup(self._html, 'lxml')
    
    
    def get_text(self):
        return self.soupify().get_text()
    
    
    def __repr__(self):
        return '<HTMLPage for "{}">'.format(self.title)


class AdamsApiPage(HTMLPage):
    def __init__(self, url=None, start_date='', end_date=''):
        if url is None:
            url = ADAMS_QUERY_STRING.format(
                start_date=start_date, end_date=end_date)
        super().__init__(url)
        if not end_date:
            end_date = datetime.now()
        if not start_date:
            try:
                end_date = datetime.strptime(end_date, '%m/%d/%Y')
            except:
                pass
            start_date = end_date - timedelta(1)
        self.start_date = start_date
        self.end_date = end_date
        self.soup = self.soupify()
        self.data = [{child.name: child.text.strip() if child.text.strip() else None
                      for child in result}
                     for result in self.soup.find_all('result')]


class PDFPage(object):
    def __init__(self, url):
        self.url = url
        with requests.Session() as s:
            r = s.get(self.url, headers=HEADERS)
            self.status_code = r.status_code
            data = r.content
        self._data = io.BytesIO(data)
        self.accession = re.search(r'ML\w{9}', self.url).group()
    
    
    def __repr__(self):
        return "<PDFPage for {}>".format(self.accession)
        
    def get_text(self):
        try:
            pdf = PyPDF2.PdfFileReader(self._data)
            return '\n'.join([pdf.getPage(p).extractText() 
                              for p in range(pdf.getNumPages())])
        except PyPDF2.utils.PdfReadError:
            return 'PDF read error'
        

class Part21YearPage(HTMLPage):
    def __init__(self, year):
        self.year = year
        self._url = 'https://www.nrc.gov/reading-rm/doc-collections/event-status/part21/{}'.format(self.year)
        super().__init__(self._url)
        self.part21List = self._get_part21_list()
        self.logNumbers = self.part21List['Log No'].tolist()
        self.shape = self.part21List.shape
    
    
    def head(self, n=5):
        return self.part21List.head(n)
    
    def __repr__(self):
        return "<Part21YearPage for {}>".format(self.year)
        
        
    def _get_part21_list(self):
        df = pd.read_html(self._html, attrs={'border':'1'}, header=0)[0]
        df.columns = ['Log No', 'Notifier', 
                      'Description', 'Report Date', 
                      'Event No. / Accession No.']
        
        soup = self.soupify()
        tableHTML = soup.find('table', attrs={'border': '1'}) 
        links = {a.text: (self._url + a['href'] if '/' not in a['href'] 
                    else 'https://www.nrc.gov' + a['href']) 
                    for a in tableHTML.select('tr td a')}
        df['Hyperlink'] = df['Log No'].map(links)
        return df
        
        
    def __add__(self, other):
        return pd.concat([self.part21List, other.part21List])
        

    def get(self, logNo):
        loc = (self.part21List['Log No'] == logNo, 'Hyperlink')
        return Part21Report(self.part21List.loc[loc].values[0]).text
   
   
    def get_all_text(self):
        self.part21List['Text'] = [Part21Report(link).text 
                for link in self.part21List['Hyperlink']]


class ENPage(HTMLPage):
    def __init__(self, url):
        super().__init__(url)
        self._html = re.sub('\n+', '\n', re.sub(r'<BR\s*?>', '\n', self._html, flags=re.IGNORECASE))
        self.soup = self.soupify()
        self.body = str(self.soup.find('div', attrs={'id': 'mainSubFull'}))
        self._enlist = enTagRegex.findall(self._html)


    def get_en_text(self, enNumber):
        body = str(self.soupify().find('div', attrs={'id': 'mainSubFull'}))
        anchors = [m.group() for m in enTagRegex.finditer(body)]
        spans = {re.search(r'\d{5}', a).group(): 
                 (body.find(a), body.find(anchors[i + 1])) 
                 if i < len(anchors)-1 else (body.find(a), len(body))
                 for i, a in enumerate(anchors)}
        
        if enNumber not in self._enlist:
            return 'EN not found'
        
        start, stop = spans[enNumber]
        soup = BeautifulSoup(body[start:stop], 'lxml')
        raw_text = soup.get_text()

        return raw_text
    
    
    def get_all_ens(self):
        return [self.get_en_text(en) for en in self._enlist]
    

    def get_en_soup(self, enNumber):
        if enNumber not in self._enlist:
            return 'EN not found'
        anchors = [m.group() for m in enTagRegex.finditer(self.body)]
        spans = {re.search(r'\d{5}', a).group(): 
                 (self.body.find(a), self.body.find(anchors[i + 1])) 
                 if i < len(anchors)-1 else (self.body.find(a), len(self.body))
                 for i, a in enumerate(anchors)}
        start, stop = spans[enNumber]
        return BeautifulSoup(self.body[start:stop], 'lxml')
        

    def get_unit_table(self, enNumber):
        if enNumber not in self._enlist:
            return 'EN not found'
        table = [table for table in self.get_en_soup(enNumber).findAll('table') 
                 if table.find('td').text == 'Unit']
        if table:
            data = {}
            unit_table = table[0]
            squarify_table(unit_table)
            header = [td.text for td in unit_table.find('tr').findAll('td')]
            for ix, row in enumerate(unit_table.findAll('tr')):
                for i, cell in enumerate(row.findAll('td')):
                    if ix == 0:
                        data[cell.text] = []
                    else:
                        key = header[i]
                        data[key].append(cell.text)
            return data
        return np.nan
    

    def parse(self):
        try:
            en_texts = [(en, self.get_en_text(en)) for en in self._enlist]
        except:
            en_texts = [(en, self.get_en_text(en)) for en in self._enlist[:-1]]
        data = []
        for num, text in en_texts:
            # try:
            enData = parse_en(text)
            # except:
            #     print('Error in ' + fpath)
            #     continue
            unitData = self.get_unit_table(num)
            enData.update({field: value for field, value in 
                zip(EN_UNIT_FIELDS, parse_unit_data(unitData))})
            enData['10 CFR Section'] = fix_cfr(enData['10 CFR Section'])
            data.append(enData)
        return data
    
    def __getitem__(self, key):
        return self.get_en_text(key)
    
    def __repr__(self):
        return '<ENPage for "{}">'.format(self.title)
    
        
class Part21Report(object):
    def __init__(self, url):
        if adamsPackageRE.search(url):
            pass
        self.url = url
        self._type = self.get_type()
        self.text = self.get_text()
    
    
    def get_type(self):
        if adamsPackageRE.search(self.url):
            return 'adams'
        if self.url.endswith('.html'):
            return 'html'
        if self.url.endswith('.pdf'):
            return 'pdf'
        if enRegex.search(self.url):
            return 'en'
        
    def get_text(self):
        if self._type == 'html':
            page = HTMLPage(self.url)
            soup = page.soupify()
            body = soup.find('pre')
            if not body:
                body = BeautifulSoup(htmlRegex.search(str(soup)).group(1), 'lxml')
            try:
                return body.get_text()
            except AttributeError:
                return 'HTML read error'
        
        if self._type == 'pdf':
            page = PDFPage(self.url)
            return page.get_text()
        
        if self._type == 'en':
            enNum = enRegex.search(self.url).group(2)[2:]
            page = ENPage(self.url)
            return page.get_en_text(enNum)
  
        if self._type == 'adams':
            page = ADAMSPackage(self.url)
            return page.text


class ADAMSPackage(HTMLPage):
    def __init__(self, url):
        super().__init__(url)
        soup = self.soupify()
        self._links = ['https://www.nrc.gov' + a['href'].replace('..', '') 
                        for a in soup.findAll('a', 
                                              attrs={'class': 'ADAMSLink'})]
        self.text = '\n'.join([PDFPage(link).get_text() for link in self._links])


