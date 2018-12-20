from lxml import etree
import datetime
import csv
import configparser
import codecs

#This Python script produces XML for Workflows. It takes a WF xml and source table list as input and generates other xmls by updating the configs, table names and session names.
#This also takes input of IC names as csv input and updates the configs accordingly
# Doesnt work at the moment. Should find a way to include multiple cdc tables in single session

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
repo_name = config[sec]['repo_name']
new_wf_name = config[sec]['new_wf_name']


# Function to read wf list
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
    root.set("REPOSITORY_VERSION", "187.96")
    return root


# Function for Repository Tag
def repo_tag():
    repo = etree.SubElement(root, "REPOSITORY")
    repo.set("NAME", repo_name)
    repo.set("VERSION", "187")
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

#WorkFlow Tag
val = read_attr("SRC", "WORKFLOW")
val[0]['NAME'] = new_wf_name
workf = etree.SubElement(fold, "WORKFLOW", attrib=val[0])

#Scheduler Tag
val = read_attr("SRC", "SCHEDULER")
sch = etree.SubElement(workf, "SCHEDULER", attrib=val[0])
val = read_attr("SRC", "SCHEDULEINFO")
schi = etree.SubElement(sch, "SCHEDULEINFO", attrib=val[0])

#Task Tag
val = read_attr("SRC", "TASK")
tsk = etree.SubElement(workf, "TASK", attrib=val[0])


wfs = read_wf()
print(wfs)

iCount = 0

for i in wfs:

    iCount = iCount + 1
    mp_name = "m_pwx_stg_sor_gis_15"
    ss_name = "s_" + mp_name
    wf_name = new_wf_name





    #Session Tag
    val = read_attr("SRC", "SESSION")
    val[0]['MAPPINGNAME'] = mp_name
    val[0]['NAME'] = ss_name
    ssn = etree.SubElement(workf, "SESSION", attrib=val[0])

    #Session Transformation Inst
    ss_trfm_attrib = field_attr("SESSION","TASKINSTANCE","SESSTRANSFORMATIONINST")
    for s in range(len(ss_trfm_attrib)):
        if ss_trfm_attrib[s]['TRANSFORMATIONTYPE'] == 'Target Definition':
            ss_trfm_attrib[s]['SINSTANCENAME'] = i.lower()
            ss_trfm_attrib[s]['TRANSFORMATIONNAME'] = i.lower()
            ss_trfm_fld = etree.SubElement(ssn, "SESSTRANSFORMATIONINST", attrib=ss_trfm_attrib[s])
            #flat_file = field_attr_name("SESSTRANSFORMATIONINST", "SESSTRANSFORMATIONINST", "FLATFILE","Target Definition")
            #flat_file_fld = etree.SubElement(ss_trfm_fld, "FLATFILE", attrib=flat_file[0])
        elif ss_trfm_attrib[s]['TRANSFORMATIONTYPE'] == 'Source Definition':
            ss_trfm_attrib[s]['SINSTANCENAME'] = i
            ss_trfm_attrib[s]['TRANSFORMATIONNAME'] = i
            ss_trfm_fld = etree.SubElement(ssn, "SESSTRANSFORMATIONINST", attrib=ss_trfm_attrib[s])
            #flat_file = field_attr_name("SESSTRANSFORMATIONINST", "SESSTRANSFORMATIONINST", "FLATFILE", "Source Definition")
            #flat_file_fld = etree.SubElement(ss_trfm_fld, "FLATFILE", attrib=flat_file[0])
            # own = etree.SubElement(ss_trfm_fld, "ATTRIBUTE")
            # own.set("NAME", "Owner Name")
            # own.set("VALUE", "$$LIBRARYNAME")

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

    # #Session Component
    # val = read_attr("SRC", "SESSIONCOMPONENT")
    # tsk_val = read_attr("SRC", "TASK")
    # #print('t',tsk_val[1])
    # for s in range(len(val)):
    #     ss_cmp_fld = etree.SubElement(ssn, "SESSIONCOMPONENT", attrib=val[s])
    #     tsk_cmp_fld = etree.SubElement(ss_cmp_fld, "TASK", attrib=tsk_val[s+1])
    #     tsk_attr = etree.SubElement(tsk_cmp_fld, "ATTRIBUTE")
    #     tsk_attr.set("NAME", "Fail task if any command fails")
    #     if (val[s]['TYPE'] == "Pre-session command" or val[s]['TYPE'] == "Post-session success command"):
    #         tsk_attr.set("VALUE", "YES")
    #     else:
    #         tsk_attr.set("VALUE", "NO")
    #     tsk_attr2 = etree.SubElement(tsk_cmp_fld, "ATTRIBUTE")
    #     tsk_attr2.set("NAME", "Recovery Strategy")
    #     if (val[s]['TYPE'] == "Pre-session command" or val[s]['TYPE'] == "Post-session success command"):
    #         tsk_attr2.set("VALUE", "Restart task")
    #     else:
    #         tsk_attr2.set("VALUE", "Fail task and continue workflow")
    #     if (val[s]['TYPE'] == "Pre-session command" or val[s]['TYPE'] == "Post-session success command"):
    #         tsk_attr3 = etree.SubElement(tsk_cmp_fld, "VALUEPAIR")
    #         tsk_attr3.set("EXECORDER", "1")
    #         if (val[s]['TYPE'] == "Pre-session command"):
    #             tsk_attr3.set("NAME", "create_filelist")
    #             value = "cd $PMSourceFileDir/learning/jam; ./crtfilelist.sh "+i+" $PMSourceFileDir/learning/jam"
    #             #print('i',i.split('_')[0])
    #         else:
    #             tsk_attr3.set("NAME", "archive")
    #             value = "cd $PMSourceFileDir/learning/jam; ./archfiles.sh filelist_" +i+" $PMSourceFileDir/learning/jam"
    #         tsk_attr3.set("REVERSEASSIGNMENT", "NO")
    #         tsk_attr3.set("VALUE", value)
    #     #tsk_ref = field_attr_name("SESSIONCOMPONENT", "SESSIONCOMPONENT", "TASK", "Pre-session command")

    #Session Extension
    val = read_attr("SRC", "SESSIONEXTENSION")
    print(val)
    for s in range(len(val)):
        if val[s]['TRANSFORMATIONTYPE'] == 'Target Definition':
            val[s]['SINSTANCENAME'] = i.lower()
            ss_ext_fld = etree.SubElement(ssn, "SESSIONEXTENSION", attrib=val[s])
            conn_ref = field_attr_name("SESSIONEXTENSION", "SESSIONEXTENSION", "CONNECTIONREFERENCE","Target Definition")
            #conn_ref[0]['CONNECTIONNAME'] = conn_name_trg
            conn_ref_fld = etree.SubElement(ss_ext_fld, "CONNECTIONREFERENCE", attrib=conn_ref[0])
            ss_ext_attr = field_attr_name("SESSIONEXTENSION", "TASKINSTANCE", "ATTRIBUTE", "Target Definition")
            for j in ss_ext_attr:
                if j.get('NAME') == "General Options":
                    break
                elif j.get('NAME') == "Merge File Name":
                    j['VALUE'] = "sor_"+i.lower()+".out"
                elif j.get('NAME') == "Output filename":
                    j['VALUE'] = "sor_"+i.lower()+"1.out"
                elif j.get('NAME') == "Reject filename":
                    j['VALUE'] = "sor_"+i.lower()+"1.bad"
                elif j.get('NAME') == "Command":
                    j['VALUE'] = "psql -h $$TGT_HOST -d $$TGT_DB -p $$TGT_PORT -U $$TGT_DB_USER -c \"SET datestyle =\"\"ISO, MDY\"\";truncate table $$TGT_SCHEMA.sor_"+i.lower()+"; copy $$TGT_SCHEMA.sor_"+i.lower()+" from STDIN WITH CSV DELIMITER \'~\' NULL as \'\'\""
                ss_attr_fld = etree.SubElement(ss_ext_fld, "ATTRIBUTE", attrib=j)
            #ss_ext_attr = field_attr_name("SESSIONEXTENSION", "SESSIONEXTENSION", "ATTRIBUTE", "Target Definition")
            #for j in ss_ext_attr:
                #ss_attr_fld = etree.SubElement(ss_ext_fld, "ATTRIBUTE", attrib=j)
        elif val[s]['TRANSFORMATIONTYPE'] == 'Source Definition':
            val[s]['SINSTANCENAME'] = i
            val[s]['DSQINSTNAME'] = "SQ_" +i
            ss_ext_fld = etree.SubElement(ssn, "SESSIONEXTENSION", attrib=val[s])
            # conn_ref = field_attr_name("SESSIONEXTENSION", "SESSIONEXTENSION", "CONNECTIONREFERENCE",
            #                            "Source Definition")
            # # conn_ref[0]['CONNECTIONNAME'] = conn_name_src
            # conn_ref_fld = etree.SubElement(ss_ext_fld, "CONNECTIONREFERENCE", attrib=conn_ref[0])
            # ss_ext_attr = field_attr_name("SESSIONEXTENSION", "TASKINSTANCE", "ATTRIBUTE", "Source Definition")
            # for j in ss_ext_attr:
            #     if j.get('NAME') == "General Options":
            #         break
            #     elif j.get('NAME') == "Source filename":
            #         j['VALUE'] = "filelist_"+i+".lst"
            #     ss_attr_fld = etree.SubElement(ss_ext_fld, "ATTRIBUTE", attrib=j)


        elif val[s]['TRANSFORMATIONTYPE'] == 'Source Qualifier':
            #val[s]['SINSTANCENAME'] = i.upper()
            val[s]['SINSTANCENAME'] = "SQ_" + i
            ss_ext_fld = etree.SubElement(ssn, "SESSIONEXTENSION", attrib=val[s])
            conn_ref = field_attr_name("SESSIONEXTENSION", "SESSIONEXTENSION", "CONNECTIONREFERENCE","Source Qualifier")
            # #conn_ref[0]['CONNECTIONNAME'] = conn_name_src
            conn_ref_fld = etree.SubElement(ss_ext_fld, "CONNECTIONREFERENCE", attrib=conn_ref[0])
            # ss_ext_attr = field_attr_name("SESSIONEXTENSION", "TASKINSTANCE", "ATTRIBUTE", "Source Definition")
            # value = "filelist_"+i.upper()+".lst"
            # print(value)
            # for j in ss_ext_attr:
            #     if j.get('NAME') == "General Options":
            #         break
            #     elif j.get('NAME') == "Source filename":
            #         #print('j',j.get('VALUE'))
            #         j['VALUE'] = value
            #     elif j.get('NAME') == "Source file directory":
            #         #print('j',j.get('VALUE'))
            #         j['VALUE'] = "$PMSourceFileDir\glassesdata\motor_vehicles\processing"
            #     ss_attr_fld = etree.SubElement(ss_ext_fld, "ATTRIBUTE", attrib=j)

    sFlag = 0
    for s in ss_ext_attr:
        if s.get('NAME') == "General Options":
            sFlag = 1
        if sFlag == 1:
            if s.get('NAME') == "Session Log File Name":
                s['VALUE']= ss_name +".log"
            ss_attr_fld = etree.SubElement(ssn, "ATTRIBUTE", attrib=s)



iCount = 0
for j in wfs:
    mp_name = prefix + "_" + j.lower()
    ss_name = "s_" + mp_name
    iCount = iCount + 1

    # Task INstance
    tsk_inst_attr = field_attr("WORKFLOW", "WORKFLOW", "TASKINSTANCE")
    for s in tsk_inst_attr:
        if s.get('TASKTYPE') == "Session":
            s['TASKNAME'] = ss_name
            s['NAME'] = ss_name
        if iCount > 1 and s.get('TASKTYPE') == "Start":
            continue
        tsk_attr_fld = etree.SubElement(workf, "TASKINSTANCE", attrib=s)

iCount = 0
for j in wfs:
    mp_name = prefix + "_" + j.lower()
    ss_name = "s_" + mp_name
    iCount = iCount + 1



    # Workflow link

    val = read_attr("SRC", "WORKFLOWLINK")
    if iCount == 1:
        val[0]['TOTASK'] = ss_name
    else:
        val[0]['FROMTASK'] = prev_ss_name
        val[0]['TOTASK'] = ss_name
        # val[0]['CONDITION'] = "$"+prev_ss_name+".Status=SUCCEEDED"
    wf_link_fld = etree.SubElement(workf, "WORKFLOWLINK", attrib=val[0])

    prev_ss_name = ss_name


iCount = 0
for j in wfs:
    mp_name = prefix + "_" + j.lower()
    ss_name = "s_" + mp_name
    iCount = iCount + 1

    wf_var_attr = field_attr("WORKFLOW", "WORKFLOW", "WORKFLOWVARIABLE")
    for s in wf_var_attr:
        if s.get('NAME').find("s_m_src_sor_verint") != -1:
            s['NAME'] = s['NAME'].replace(fname, ss_name)
        elif (s.get('NAME').find("Start") != -1 and iCount > 1) or  (s.get('NAME').find("$$LIBRARYNAME") != -1 and iCount > 1):
            continue
        wf_var_fld = etree.SubElement(workf, "WORKFLOWVARIABLE", attrib=s)


#Workflow attributes
wf_attr = field_attr("WORKFLOWVARIABLE", "WORKFLOW", "ATTRIBUTE")
for s in wf_attr:
    if s.get('NAME') == "Workflow Log File Name":
        s['VALUE'] = new_wf_name+".log"
    wf_attr_fld = etree.SubElement(workf, "ATTRIBUTE", attrib=s)

tree = etree.ElementTree(root)
outfile_name = "wf_combined_verint" + ".XML"
tree.write(outfile_name, pretty_print=True, encoding="UTF-8", xml_declaration=True,
           doctype="""<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">""")