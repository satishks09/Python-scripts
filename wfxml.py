from lxml import etree
import datetime
import csv
import configparser
import codecs

#This Python script produces XML for Workflows. It takes a WF xml and source table list as input and generates other xmls by updating the configs, table names and session names.
#This also takes input of IC names as csv input and updates the configs accordingly

# Get Current Date
now = datetime.datetime.now()
now_str = now.strftime("%Y-%m-%d %H:%M:%S")
print(now.strftime("%Y-%m-%d %H:%M:%S"))


# config
sec = "SectionFive"
config = configparser.ConfigParser()
config.sections()
config.read('config.ini')

src_fname = config[sec]['src_fname']
fname = config[sec]['fname']
wf_list = config[sec]['wf_list']
ic_names = config[sec]['ic_names']
db2_sub_system_id = config[sec]['db2_sub_system_id']
db2_pwx_schema = config[sec]['db2_pwx_schema']
prefix = config[sec]['prefix']
trg_prefix = config[sec]['trg_prefix']
exp_ctl_fname = config[sec]['exp_ctl_fname']
conn_name_trg = config[sec]['conn_name_trg']
conn_name_src = config[sec]['conn_name_src']


# Function to read pkmd5 from csv
def read_wf():
    fn = wf_list
    wfs = []
    i=0
    with codecs.open(fn, "r", encoding="utf8", errors='ignore') as f:
        reader = csv.reader(f, delimiter=",", dialect='excel')

        for row in reader:

            wfs.append(row[0].strip())

        return wfs

def read_icname(table_name):
    fn = ic_names
    with codecs.open(fn, "r", encoding="utf8", errors='ignore') as f:
        reader = csv.reader(f, delimiter=",", dialect='excel')
        for row in reader:
            if row[0].strip() == table_name:
                return row[1]

# Function for Powermart Tag
def powermart_tag():
    root = etree.Element("POWERMART")
    root.set("CREATION_DATE", now_str)
    root.set("REPOSITORY_VERSION", "184.93")
    return root


# Function for Repository Tag
def repo_tag():
    repo = etree.SubElement(root, "REPOSITORY")
    repo.set("NAME", "REPO_DEV")
    repo.set("VERSION", "184")
    repo.set("CODEPAGE", "UTF-8")
    repo.set("DATABASETYPE", "Oracle")
    return repo


# Function to read Element Attributes
def read_attr(fl_type, elem):
    attrib = []
    if fl_type == "SRC":
        doc = etree.parse(src_fname)
    elif fl_type == "TRG":
        doc = etree.parse(trg_sor_fname)
    elif fl_type == "TRG_CDC":
        doc = etree.parse(trg_cdc_fname)
    elif fl_type == "TRF":
        doc = etree.parse(exp_ctl_fname)
    root = doc.getroot()
    for s in root.iter(elem):
        attrib.append(s.attrib)
    return attrib


def field_attr(tag_name,tag_end,tag_field):
    src_attrib = []
    iFlag = 0
    doc = etree.parse(src_fname)
    elem_root = doc.getroot()
    for e in elem_root.iter():
        if (e.tag == tag_name):
            iFlag = 1
        elif (e.tag == tag_end):
            iFlag = 0
        if (e.tag == tag_field) and iFlag == 1:
            src_attrib.append(e.attrib)
    return src_attrib

def field_attr_name(tag_name,tag_end,tag_field,name):
    src_attrib = []
    iFlag = 0
    doc = etree.parse(src_fname)
    elem_root = doc.getroot()
    for e in elem_root.iter():
        if (e.tag == tag_name and e.get('TRANSFORMATIONTYPE') ==name):
            iFlag = 1
        elif (e.tag == tag_end):
            iFlag = 0
        if (e.tag == tag_field) and iFlag == 1:
            src_attrib.append(e.attrib)
    return src_attrib


# Powermart Tag
root = powermart_tag()

# Repository Tag
repo = repo_tag()

# Folder Tag
val = read_attr("SRC", "FOLDER")
fold = etree.SubElement(repo, "FOLDER", attrib=val[0])

# Config Tag
val = read_attr("SRC", "CONFIG")
conf = etree.SubElement(fold, "CONFIG", attrib=val[0])

# Config Attributes
conf_attrib = field_attr("CONFIG","WORKFLOW","ATTRIBUTE")

for s in range(len(conf_attrib)):
    conf_fld = etree.SubElement(conf, "ATTRIBUTE",attrib=conf_attrib[s])


wfs = read_wf()
print(wfs)

for i in wfs:

    mp_name = prefix + "_" + i.lower()
    ss_name = "s_" + mp_name
    wf_name = "wf_" + mp_name

    #WorkFlow Tag
    val = read_attr("SRC", "WORKFLOW")
    val[0]['NAME'] = wf_name
    workf = etree.SubElement(fold, "WORKFLOW", attrib=val[0])

    #Scheduler Tag
    val = read_attr("SRC", "SCHEDULER")
    sch = etree.SubElement(workf, "SCHEDULER", attrib=val[0])
    val = read_attr("SRC", "SCHEDULEINFO")
    schi = etree.SubElement(sch, "SCHEDULEINFO", attrib=val[0])

    #Task Tag
    val = read_attr("SRC", "TASK")
    tsk = etree.SubElement(workf, "TASK", attrib=val[0])

    #Session Tag
    val = read_attr("SRC", "SESSION")
    val[0]['MAPPINGNAME'] = mp_name
    val[0]['NAME'] = ss_name
    ssn = etree.SubElement(workf, "SESSION", attrib=val[0])

    #Session Transformation Inst
    ss_trfm_attrib = field_attr("SESSION","TASKINSTANCE","SESSTRANSFORMATIONINST")
    for s in range(len(ss_trfm_attrib)):
        if ss_trfm_attrib[s]['TRANSFORMATIONTYPE'] == 'Target Definition':
            ss_trfm_attrib[s]['SINSTANCENAME'] =  i.lower()
            ss_trfm_attrib[s]['TRANSFORMATIONNAME'] =  i.lower()
            ss_trfm_fld = etree.SubElement(ssn, "SESSTRANSFORMATIONINST", attrib=ss_trfm_attrib[s])
        elif ss_trfm_attrib[s]['TRANSFORMATIONTYPE'] == 'Source Definition':
            ss_trfm_attrib[s]['SINSTANCENAME'] = i
            ss_trfm_attrib[s]['TRANSFORMATIONNAME'] = i
            ss_trfm_fld = etree.SubElement(ssn, "SESSTRANSFORMATIONINST", attrib=ss_trfm_attrib[s])
        elif ss_trfm_attrib[s]['TRANSFORMATIONTYPE'] == 'Source Qualifier':
            ss_trfm_attrib[s]['SINSTANCENAME'] = "SQ_" + i
            ss_trfm_attrib[s]['TRANSFORMATIONNAME'] = "SQ_" + i
            ss_trfm_fld = etree.SubElement(ssn, "SESSTRANSFORMATIONINST", attrib=ss_trfm_attrib[s])
        else:
            ss_trfm_fld = etree.SubElement(ssn, "SESSTRANSFORMATIONINST", attrib=ss_trfm_attrib[s])
            par = etree.SubElement(ss_trfm_fld, "PARTITION")
            par.set("DESCRIPTION", "")
            par.set("NAME", "Partition #1")

    #Config Reference
    con_ref = etree.SubElement(ssn, "CONFIGREFERENCE")
    con_ref.set("REFOBJECTNAME", "default_session_config")
    con_ref.set("TYPE", "Session config")

    #Session Extension
    val = read_attr("SRC", "SESSIONEXTENSION")
    print(val)
    for s in range(len(val)):
        if val[s]['TRANSFORMATIONTYPE'] == 'Target Definition':
            val[s]['SINSTANCENAME'] =  i.lower()
            ss_ext_fld = etree.SubElement(ssn, "SESSIONEXTENSION", attrib=val[s])
            conn_ref = field_attr_name("SESSIONEXTENSION", "SESSIONEXTENSION", "CONNECTIONREFERENCE","Target Definition")
            conn_ref[0]['CONNECTIONNAME'] = conn_name_trg
            conn_ref_fld = etree.SubElement(ss_ext_fld, "CONNECTIONREFERENCE", attrib=conn_ref[0])
            ss_ext_attr = field_attr_name("SESSIONEXTENSION", "SESSIONEXTENSION", "ATTRIBUTE", "Target Definition")
            for j in ss_ext_attr:
                ss_attr_fld = etree.SubElement(ss_ext_fld, "ATTRIBUTE", attrib=j)
        elif val[s]['TRANSFORMATIONTYPE'] == 'Source Definition':
            val[s]['SINSTANCENAME'] = i
            val[s]['DSQINSTNAME'] = "SQ_" +i
            ss_ext_fld = etree.SubElement(ssn, "SESSIONEXTENSION", attrib=val[s])
        elif val[s]['TRANSFORMATIONTYPE'] == 'Source Qualifier':
            val[s]['SINSTANCENAME'] = "SQ_" +i
            ss_ext_fld = etree.SubElement(ssn, "SESSIONEXTENSION", attrib=val[s])
            conn_ref = field_attr_name("SESSIONEXTENSION", "SESSIONEXTENSION", "CONNECTIONREFERENCE","Source Qualifier")
            conn_ref[0]['CONNECTIONNAME'] = conn_name_src
            conn_ref_fld = etree.SubElement(ss_ext_fld, "CONNECTIONREFERENCE", attrib=conn_ref[0])
            ss_ext_attr = field_attr_name("SESSIONEXTENSION", "TASKINSTANCE", "ATTRIBUTE", "Source Qualifier")
            for j in ss_ext_attr:
                if j.get('NAME') == "General Options":
                    break
                ss_attr_fld = etree.SubElement(ss_ext_fld, "ATTRIBUTE", attrib=j)

    sFlag = 0
    for s in ss_ext_attr:
        if s.get('NAME') == "General Options":
            sFlag = 1
        if sFlag == 1:
            if s.get('NAME') == "Session Log File Name":
                s['VALUE']= ss_name +".log"
            ss_attr_fld = etree.SubElement(ssn, "ATTRIBUTE", attrib=s)

    #Task INstance
    tsk_inst_attr = field_attr("WORKFLOW", "WORKFLOW", "TASKINSTANCE")
    for s in tsk_inst_attr:
        if s.get('TASKTYPE') == "Session":
            s['TASKNAME'] = ss_name
            s['NAME'] = ss_name
        tsk_attr_fld = etree.SubElement(workf, "TASKINSTANCE", attrib=s)

    #Workflow link
    val = read_attr("SRC", "WORKFLOWLINK")
    val[0]['TOTASK'] = ss_name
    wf_link_fld = etree.SubElement(workf, "WORKFLOWLINK", attrib=val[0])

    #Workflow variable
    wf_var_attr = field_attr("WORKFLOW", "WORKFLOW", "WORKFLOWVARIABLE")
    for s in wf_var_attr:
        if s.get('NAME').find("m_src_sor") != -1:
            s['NAME'] = s['NAME'].replace(fname,ss_name)
        elif s.get('NAME') == "$$DB2_IC_NAME":
            print(i)
            s['DEFAULTVALUE'] = read_icname(i)
        elif s.get('NAME') == "$$DB2_SUB_SYSTEM_ID":
            s['DEFAULTVALUE'] = db2_sub_system_id
        elif s.get('NAME') == "$$DB2_PWX_SCHEMA":
            s['DEFAULTVALUE'] = db2_pwx_schema
        wf_var_fld = etree.SubElement(workf, "WORKFLOWVARIABLE", attrib=s)

    #Workflow attributes
    wf_attr = field_attr("WORKFLOWVARIABLE", "WORKFLOW", "ATTRIBUTE")
    for s in wf_attr:
        if s.get('NAME') == "Workflow Log File Name":
            s['VALUE'] = wf_name+".log"
        wf_attr_fld = etree.SubElement(workf, "ATTRIBUTE", attrib=s)







tree = etree.ElementTree(root)
outfile_name = "wf_combined_ced" + ".XML"
tree.write(outfile_name, pretty_print=True, encoding="UTF-8", xml_declaration=True,
           doctype="""<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">""")