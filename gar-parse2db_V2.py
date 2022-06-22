import psycopg2
from lxml import etree
import os
import re
#COUNT=-1                         # количество файлов для парсинга (-1 все файлы каталога) !!! НЕ НУЖНО, убрать !!!
EXCUDE_TAGS=[]                  # тэги, которые не нужно парсить (getTreeByXml)

# ПОДГОТОВКА СПИСКА ФАЙЛОВ ДЛЯ ПАРСИНГА
PATH4PARSE=[
    'C:\\tmp\\fias\\rostov', 
    ]
DATA4PARSE=[]
for path in PATH4PARSE:
    d={}
    for address, dirs, files in os.walk(path):
        for name in files:
            filepath=os.path.join(address, name)
            '''
            #--------загрузка справочников (единожны)
            if re.search('AS_ADDR_OBJ_TYPES_\d',filepath):
                d['ADDRESSOBJECTTYPE']=filepath
            if re.search('AS_APARTMENT_TYPES_\d',filepath):
                d['APARTMENTTYPE']=filepath
            if re.search('AS_HOUSE_TYPES_\d',filepath):
                d['HOUSETYPE']=filepath
            if re.search('AS_ROOM_TYPES_\d',filepath):
                d['ROOMTYPE']=filepath

            #if re.search('AS_PARAM_TYPES_\d',filepath):
            #    d['PARAMTYPE']=filepath
            #---------- загрузка объектов
            if re.search('AS_HOUSES_\d',filepath):
                d['HOUSE']=filepath
            if re.search('AS_ADDR_OBJ_\d',filepath):
                d['OBJECT']=filepath
            if re.search('AS_APARTMENTS_\d',filepath):
                d['APARTMENT']=filepath
            if re.search('AS_ROOMS_\d',filepath):
                d['ROOM']=filepath
            '''
            #--------- загрузка иерархий
            # !!! это запускать вторым этапом когда все объекты и справочники загружены            
            if 'AS_ADM_HIERARCHY' in filepath:
                d['ITEM_ADM']=filepath
            
    DATA4PARSE.append(d)

conn = psycopg2.connect(host='host',
                        port='port',
                        user='user',
                        password='password',
                        database='dbname')
cursor = conn.cursor()


def getTreeByXml(file_object, RECORD_TAG):
    record={}
    context = etree.iterparse(file_object, events=("start","end"))
    for event, elem in context:
        attrib=dict(elem.attrib)
        tag=etree.QName(elem.tag).localname
        if not attrib or tag in EXCUDE_TAGS:
            continue
        if event=="end" and tag==RECORD_TAG:
            yield record
            elem.clear()
            record={}
            continue
        record.setdefault(tag, attrib)
        if record[tag] != attrib:
            try:
                record[tag].append(attrib)
            except:
                record[tag]=[record[tag], attrib]


for region in DATA4PARSE:
    for table_name, filename in region.items():
        print ('\r\n',filename)
        fr=open(filename,'rb')
        record_tag = table_name
        if table_name == 'ITEM_ADM':
            record_tag = 'ITEM'
        
        COUNT=-1
        
        for rec in getTreeByXml(fr, record_tag):
            if not COUNT:
                break
            rec=rec[record_tag]
            print(f'\rОбработано: ({COUNT})', end='')

            #----------подготовка данных для вставки в sql-запрос
            if 'PATH' in rec:
                rec['PATH']= '{'+rec['PATH'].replace('.', ',')+'}'
            fields_name = '"'+'", "'.join(rec.keys())+'"'
            fields_name=fields_name.lower().replace('"id"', '"id_"').replace('"desc"', '"desc_"')
            fields_values = "'"+"', '".join(rec.values())+"'"

            cursor.execute(f'INSERT INTO gar_{table_name} ({fields_name}) VALUES({fields_values})') 
            COUNT-=1
        conn.commit() 
cursor.close()
conn.close()
