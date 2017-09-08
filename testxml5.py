from lxml import etree
import datetime
import csv
import configparser

# This Python Script Produces one xml for all the mappings combined. The mapping is for CDC load from Source to Target.
# This is a modified version of testxml3

# Get Current Date
now = datetime.datetime.now()
now_str = now.strftime("%Y-%m-%d %H:%M:%S")
print(now.strftime("%Y-%m-%d %H:%M:%S"))

# config
sec = "SectionFour"
config = configparser.ConfigParser()
config.sections()
config.read('config.ini')

src_fname = config[sec]['src_fname']
trg_cdc_fname = config[sec]['trg_cdc_fname']
trg_sor_fname = config[sec]['trg_sor_fname']
exp_ctl_fname = config[sec]['exp_ctl_fname']
pkmd5_csv = config[sec]['pkmd5_csv']
repo_name = config[sec]['repo_name']
map_name_c = config[sec]['map_name_c']


# Function to read pkmd5 from csv
def read_pkmd5(table_name):
    fn = "pkmd5_sor_pms.csv"
    with open(fn, "r", encoding="utf8") as f:
        reader = csv.reader(f, delimiter=",", dialect='excel')
        for row in reader:
            if row[0] == table_name:
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


def field_attr(fl_type, name):
    src_attrib = []
    iFlag = 0
    if fl_type == "SRC":
        doc = etree.parse(src_fname)
    elif fl_type == "TRG":
        doc = etree.parse(trg_sor_fname)
    elif fl_type == "TRG_CDC":
        doc = etree.parse(trg_cdc_fname)
    elem_root = doc.getroot()
    for e in elem_root.iter():
        if (e.tag == 'SOURCE' or e.tag == 'TARGET') and e.get('NAME') == name:
            iFlag = 1
        elif (e.tag == 'SOURCE' or e.tag == 'TARGET'):
            iFlag = 0
        if (e.tag == 'SOURCEFIELD' or e.tag == 'TARGETFIELD') and iFlag == 1:
            src_attrib.append(e.attrib)
    return src_attrib


sources = read_attr("SRC", "SOURCE")
cnt = 0

# Powermart Tag
root = powermart_tag()

# Repository Tag
repo = repo_tag()

# Folder Tag
val = read_attr("SRC", "FOLDER")
fold = etree.SubElement(repo, "FOLDER", attrib=val[0])

# Mapping Tag
map_name = map_name_c
map = etree.SubElement(fold, "MAPPING")
map.set("DESCRIPTION", "")
map.set("ISVALID", "YES")
map.set("NAME", map_name)
map.set("OBJECTVERSION", "1")
map.set("VERSIONNUMBER", "1")

for i in sources:

    src_name = i.get('NAME')
    words = src_name.split('_')
    t_name = '_'.join(words[1:])
    trg_name = "sor_" + t_name.lower()
    trg_name_cdc = t_name.lower() + "_cdc"
    print(trg_name)
    cnt +=1

    # SourceField
    src_attrib = field_attr("SRC", src_name)

    # TargetField
    trg_attrib = field_attr("TRG", trg_name)

    # TransformField Tag
    tfm_val = read_attr("TRF", "TRANSFORMFIELD")


    # SQ_Transformation
    sq_name = 'AMGDSQ_' + src_name
    sq_tfm = etree.SubElement(map, "TRANSFORMATION")
    sq_tfm.set("DESCRIPTION", "")
    sq_tfm.set("NAME", sq_name)
    sq_tfm.set("OBJECTVERSION", "1")
    sq_tfm.set("REF_DBD_NAME", i.get('DBDNAME'))
    sq_tfm.set("REF_SOURCE_NAME", src_name)
    sq_tfm.set("REUSABLE", "NO")
    sq_tfm.set("TYPE", "App Multi-Group Source Qualifier")
    sq_tfm.set("VERSIONNUMBER", "1")

    #Group
    grp = etree.SubElement(sq_tfm, "GROUP")
    grp.set("DESCRIPTION", "")
    grp.set("NAME", src_name)
    grp.set("ORDER", "1")
    grp.set("TYPE", "INPUT/OUTPUT")

    # SQ_TransformField
    for s in src_attrib:
        sq_tfm_fld = etree.SubElement(sq_tfm, "TRANSFORMFIELD")
        datatype = s.get('DATATYPE')
        precision = s.get('PRECISION')
        scale = s.get('SCALE')
        if (datatype.lower() == "varchar" or datatype.lower() == "char"):
            datatype = "string"
        elif datatype.lower() == "num32":
            datatype = "integer"
        elif datatype.lower() == "date":
            datatype = "date/time"
            precision = "32"
            scale = "9"
        sq_tfm_fld.set("DATATYPE", datatype)
        sq_tfm_fld.set("DEFAULTVALUE", "")
        sq_tfm_fld.set("DESCRIPTION", "")
        sq_tfm_fld.set("GROUP", src_name)
        sq_tfm_fld.set("NAME", s.get('NAME'))
        sq_tfm_fld.set("PICTURETEXT", s.get('PICTURETEXT'))
        sq_tfm_fld.set("PORTTYPE", "INPUT/OUTPUT")
        sq_tfm_fld.set("PRECISION", precision)
        sq_tfm_fld.set("REF_SOURCE_FIELD", s.get('NAME'))
        sq_tfm_fld.set("SCALE", scale)

    # SQ_TableAttribute
    sq_tbl_attr = etree.SubElement(sq_tfm, "TABLEATTRIBUTE")
    sq_tbl_attr.set("NAME", "Tracing Level")
    sq_tbl_attr.set("VALUE", "Normal")
    sq_tbl_attr = etree.SubElement(sq_tfm, "TABLEATTRIBUTE")
    sq_tbl_attr.set("NAME", "Output Is Deterministic")
    sq_tbl_attr.set("VALUE", "YES")
    sq_tbl_attr = etree.SubElement(sq_tfm, "TABLEATTRIBUTE")
    sq_tbl_attr.set("NAME", "Output Is Ordered")
    sq_tbl_attr.set("VALUE", "YES")

    # GetMd5 Transformation
    getmd5_tfm = etree.SubElement(map, "TRANSFORMATION")
    getmd5_tfm.set("DESCRIPTION", "")
    getmd5_tfm.set("NAME", "EXP_Get_Md5" + str(cnt))
    getmd5_tfm.set("OBJECTVERSION", "1")
    getmd5_tfm.set("REUSABLE", "NO")
    getmd5_tfm.set("TYPE", "Expression")
    getmd5_tfm.set("VERSIONNUMBER", "1")

    # Getmd5 Transform field
    pk_attrib = []
    for g in trg_attrib:
        if g.get('KEYTYPE') == "PRIMARY KEY":
            for j in src_attrib:
                if j.get('NAME').lower() == g.get('NAME'):
                    pk_attrib.append(j)
                    getmd5_tfm_fld = etree.SubElement(getmd5_tfm, "TRANSFORMFIELD")
                    datatype = j.get('DATATYPE')
                    precision = j.get('PRECISION')
                    scale = j.get('SCALE')
                    if (datatype.lower() == "varchar" or datatype.lower() == "char"):
                        datatype = "string"
                    elif datatype.lower() == "num32":
                        datatype = "integer"
                    elif datatype.lower() == "date":
                        datatype = "date/time"
                        precision = "32"
                        scale = "9"
                    getmd5_tfm_fld.set("DATATYPE", datatype)
                    getmd5_tfm_fld.set("DEFAULTVALUE", "")
                    getmd5_tfm_fld.set("DESCRIPTION", "")
                    getmd5_tfm_fld.set("NAME", j.get('NAME'))
                    getmd5_tfm_fld.set("PICTURETEXT", j.get('PICTURETEXT'))
                    getmd5_tfm_fld.set("PORTTYPE", "INPUT")
                    getmd5_tfm_fld.set("PRECISION", precision)
                    getmd5_tfm_fld.set("SCALE", scale)

    getmd5_tfm_fld_pkmd5 = etree.SubElement(getmd5_tfm, "TRANSFORMFIELD")
    getmd5_tfm_fld_pkmd5.set("DATATYPE", "string")
    getmd5_tfm_fld_pkmd5.set("DEFAULTVALUE", "")
    getmd5_tfm_fld_pkmd5.set("DESCRIPTION", "")
    getmd5_tfm_fld_pkmd5.set("EXPRESSION", read_pkmd5(trg_name))
    getmd5_tfm_fld_pkmd5.set("EXPRESSIONTYPE", "GENERAL")
    getmd5_tfm_fld_pkmd5.set("NAME", "pk_md5")
    getmd5_tfm_fld_pkmd5.set("PICTURETEXT", "")
    getmd5_tfm_fld_pkmd5.set("PORTTYPE", "OUTPUT")
    getmd5_tfm_fld_pkmd5.set("PRECISION", "32")
    getmd5_tfm_fld_pkmd5.set("SCALE", "0")

    getmd5_tbl_attr_pkmd5 = etree.SubElement(getmd5_tfm, "TABLEATTRIBUTE")
    getmd5_tbl_attr_pkmd5.set("NAME", "Tracing Level")
    getmd5_tbl_attr_pkmd5.set("VALUE", "Normal")

    # US_Transformation
    us_tfm = etree.SubElement(map, "TRANSFORMATION")
    us_tfm.set("DESCRIPTION", "")
    us_tfm.set("NAME", "UPD_CDC" + str(cnt))
    us_tfm.set("OBJECTVERSION", "1")
    us_tfm.set("REUSABLE", "NO")
    us_tfm.set("TYPE", "Update Strategy")
    us_tfm.set("VERSIONNUMBER", "1")

    us_attrib = []
    # US_TransformField
    for s in tfm_val:
        if s.get('NAME') == "dtl__capxtimestamp_in" or s.get('NAME') == "dtl__capxtimestamp_out":
            continue
        us_tfm_fld = etree.SubElement(us_tfm, "TRANSFORMFIELD")
        datatype = s.get('DATATYPE')
        precision = s.get('PRECISION')
        scale = s.get('SCALE')
        if (datatype.lower() == "varchar" or datatype.lower() == "char"):
            datatype = "string"
        elif datatype.lower() == "num32":
            datatype = "integer"
        elif datatype.lower() == "date":
            datatype = "date/time"
            precision = "32"
            scale = "9"
        us_tfm_fld.set("DATATYPE", datatype)
        us_tfm_fld.set("DEFAULTVALUE", "")
        us_tfm_fld.set("DESCRIPTION", "")
        us_tfm_fld.set("NAME", s.get('NAME'))
        us_tfm_fld.set("PICTURETEXT", s.get('PICTURETEXT'))
        us_tfm_fld.set("PORTTYPE", "INPUT/OUTPUT")
        us_tfm_fld.set("PRECISION", precision)
        us_tfm_fld.set("SCALE", scale)
        us_attrib.append(s)

    for s in src_attrib:
        if s.get('NAME') == "DTL__CAPXTIMESTAMP":
            continue
        us_tfm_fld = etree.SubElement(us_tfm, "TRANSFORMFIELD")
        datatype = s.get('DATATYPE')
        precision = s.get('PRECISION')
        scale = s.get('SCALE')
        if (datatype.lower() == "varchar" or datatype.lower() == "char"):
            datatype = "string"
        elif datatype.lower() == "num32":
            datatype = "integer"
        elif datatype.lower() == "date":
            datatype = "date/time"
            precision = "32"
            scale = "9"
        us_tfm_fld.set("DATATYPE", datatype)
        us_tfm_fld.set("DEFAULTVALUE", "")
        us_tfm_fld.set("DESCRIPTION", "")
        us_tfm_fld.set("NAME", s.get('NAME'))
        us_tfm_fld.set("PICTURETEXT", s.get('PICTURETEXT'))
        us_tfm_fld.set("PORTTYPE", "INPUT/OUTPUT")
        us_tfm_fld.set("PRECISION", precision)
        us_tfm_fld.set("SCALE", scale)
        us_attrib.append(s)

    us_tfm_fld = etree.SubElement(us_tfm, "TRANSFORMFIELD")
    us_tfm_fld.set("DATATYPE", "string")
    us_tfm_fld.set("DEFAULTVALUE", "")
    us_tfm_fld.set("DESCRIPTION", "")
    us_tfm_fld.set("NAME", "pk_md5")
    us_tfm_fld.set("PICTURETEXT", "")
    us_tfm_fld.set("PORTTYPE", "INPUT/OUTPUT")
    us_tfm_fld.set("PRECISION", "32")
    us_tfm_fld.set("SCALE", "0")

    us_tfm_fld = etree.SubElement(us_tfm, "TABLEATTRIBUTE")
    us_tfm_fld.set("NAME", "Update Strategy Expression")
    us_tfm_fld.set("VALUE", "DD_INSERT")
    us_tfm_fld = etree.SubElement(us_tfm, "TABLEATTRIBUTE")
    us_tfm_fld.set("NAME", "Forward Rejected Rows")
    us_tfm_fld.set("VALUE", "YES")
    us_tfm_fld = etree.SubElement(us_tfm, "TABLEATTRIBUTE")
    us_tfm_fld.set("NAME", "Tracing Level")
    us_tfm_fld.set("VALUE", "Normal")

    # Instances
    inst_trg_def = etree.SubElement(map, "INSTANCE")
    inst_trg_def.set("DESCRIPTION", "")
    inst_trg_def.set("NAME", trg_name_cdc)
    inst_trg_def.set("TRANSFORMATION_NAME", trg_name_cdc)
    inst_trg_def.set("TRANSFORMATION_TYPE", "Target Definition")
    inst_trg_def.set("TYPE", "TARGET")

    inst_trg_def = etree.SubElement(map, "INSTANCE")
    inst_trg_def.set("DESCRIPTION", "")
    inst_trg_def.set("NAME", trg_name)
    inst_trg_def.set("TRANSFORMATION_NAME", trg_name)
    inst_trg_def.set("TRANSFORMATION_TYPE", "Target Definition")
    inst_trg_def.set("TYPE", "TARGET")

    inst_src_def = etree.SubElement(map, "INSTANCE")
    inst_src_def.set("DBDNAME", "DB2zOs_CDC")
    inst_src_def.set("DESCRIPTION", "")
    inst_src_def.set("NAME", src_name)
    inst_src_def.set("TRANSFORMATION_NAME", src_name)
    inst_src_def.set("TRANSFORMATION_TYPE", "Source Definition")
    inst_src_def.set("TYPE", "SOURCE")

    inst_sq = etree.SubElement(map, "INSTANCE")
    inst_sq.set("DESCRIPTION", "")
    inst_sq.set("NAME", sq_name)
    inst_sq.set("REUSABLE", "NO")
    inst_sq.set("TRANSFORMATION_NAME", sq_name)
    inst_sq.set("TRANSFORMATION_TYPE", "App Multi-Group Source Qualifier")
    inst_sq.set("TYPE", "TRANSFORMATION")

    inst_exp_ctl = etree.SubElement(map, "INSTANCE")
    inst_exp_ctl.set("DESCRIPTION", "")
    inst_exp_ctl.set("NAME", "Exp_Ctl_Cols" + str(cnt))
    inst_exp_ctl.set("REUSABLE", "YES")
    inst_exp_ctl.set("TRANSFORMATION_NAME", "Exp_Ctl_Cols")
    inst_exp_ctl.set("TRANSFORMATION_TYPE", "Expression")
    inst_exp_ctl.set("TYPE", "TRANSFORMATION")

    inst_exp_md5 = etree.SubElement(map, "INSTANCE")
    inst_exp_md5.set("DESCRIPTION", "")
    inst_exp_md5.set("NAME", "EXP_Get_Md5" + str(cnt))
    inst_exp_md5.set("REUSABLE", "NO")
    inst_exp_md5.set("TRANSFORMATION_NAME", "EXP_Get_Md5" + str(cnt))
    inst_exp_md5.set("TRANSFORMATION_TYPE", "Expression")
    inst_exp_md5.set("TYPE", "TRANSFORMATION")

    inst_us = etree.SubElement(map, "INSTANCE")
    inst_us.set("DESCRIPTION", "")
    inst_us.set("NAME", "UPD_CDC" + str(cnt))
    inst_us.set("REUSABLE", "NO")
    inst_us.set("TRANSFORMATION_NAME", "UPD_CDC" + str(cnt))
    inst_us.set("TRANSFORMATION_TYPE", "Update Strategy")
    inst_us.set("TYPE", "TRANSFORMATION")

    # connector
    for c in tfm_val:
        if c.get('PORTTYPE') == "OUTPUT":
            conn = etree.SubElement(map, "CONNECTOR")
            conn.set("FROMFIELD", c.get('NAME'))
            conn.set("FROMINSTANCE", "Exp_Ctl_Cols" + str(cnt))
            conn.set("FROMINSTANCETYPE", "Expression")
            conn.set("TOFIELD", c.get('NAME'))
            conn.set("TOINSTANCE", trg_name)
            conn.set("TOINSTANCETYPE", "Target Definition")

    conn_pkmd5 = etree.SubElement(map, "CONNECTOR")
    conn_pkmd5.set("FROMFIELD", "pk_md5")
    conn_pkmd5.set("FROMINSTANCE", "EXP_Get_Md5" + str(cnt))
    conn_pkmd5.set("FROMINSTANCETYPE", "Expression")
    conn_pkmd5.set("TOFIELD", "pk_md5")
    conn_pkmd5.set("TOINSTANCE", trg_name)
    conn_pkmd5.set("TOINSTANCETYPE", "Target Definition")

    for s in src_attrib:
        if s.get('NAME') != "DTL__CAPXTIMESTAMP":
            conn_sq_trg = etree.SubElement(map, "CONNECTOR")
            conn_sq_trg.set("FROMFIELD", s.get('NAME'))
            conn_sq_trg.set("FROMINSTANCE", sq_name)
            conn_sq_trg.set("FROMINSTANCETYPE", "App Multi-Group Source Qualifier")
            conn_sq_trg.set("TOFIELD", s.get('NAME').lower())
            conn_sq_trg.set("TOINSTANCE", trg_name)
            conn_sq_trg.set("TOINSTANCETYPE", "Target Definition")

    for s in src_attrib:
        conn_sd_sq = etree.SubElement(map, "CONNECTOR")
        conn_sd_sq.set("FROMFIELD", s.get('NAME'))
        conn_sd_sq.set("FROMINSTANCE", src_name)
        conn_sd_sq.set("FROMINSTANCETYPE", "Source Definition")
        conn_sd_sq.set("TOFIELD", s.get('NAME'))
        conn_sd_sq.set("TOINSTANCE", sq_name)
        conn_sd_sq.set("TOINSTANCETYPE", "App Multi-Group Source Qualifier")

    conn_sq_ctl = etree.SubElement(map, "CONNECTOR")
    conn_sq_ctl.set("FROMFIELD", "DTL__CAPXTIMESTAMP")
    conn_sq_ctl.set("FROMINSTANCE", sq_name)
    conn_sq_ctl.set("FROMINSTANCETYPE", "App Multi-Group Source Qualifier")
    conn_sq_ctl.set("TOFIELD", "dtl__capxtimestamp_in")
    conn_sq_ctl.set("TOINSTANCE", "Exp_Ctl_Cols" + str(cnt))
    conn_sq_ctl.set("TOINSTANCETYPE", "Expression")

    for s in pk_attrib:
        conn_sq_md5 = etree.SubElement(map, "CONNECTOR")
        conn_sq_md5.set("FROMFIELD", s.get('NAME'))
        conn_sq_md5.set("FROMINSTANCE", sq_name)
        conn_sq_md5.set("FROMINSTANCETYPE", "App Multi-Group Source Qualifier")
        conn_sq_md5.set("TOFIELD", s.get('NAME'))
        conn_sq_md5.set("TOINSTANCE", "EXP_Get_Md5" + str(cnt))
        conn_sq_md5.set("TOINSTANCETYPE", "Expression")

    conn_pkmd5 = etree.SubElement(map, "CONNECTOR")
    conn_pkmd5.set("FROMFIELD", "pk_md5")
    conn_pkmd5.set("FROMINSTANCE", "EXP_Get_Md5" + str(cnt))
    conn_pkmd5.set("FROMINSTANCETYPE", "Expression")
    conn_pkmd5.set("TOFIELD", "pk_md5")
    conn_pkmd5.set("TOINSTANCE", "UPD_CDC" + str(cnt))
    conn_pkmd5.set("TOINSTANCETYPE", "Update Strategy")

    for c in tfm_val:
        if c.get('PORTTYPE') == "OUTPUT":
            conn = etree.SubElement(map, "CONNECTOR")
            conn.set("FROMFIELD", c.get('NAME'))
            conn.set("FROMINSTANCE", "Exp_Ctl_Cols" + str(cnt))
            conn.set("FROMINSTANCETYPE", "Expression")
            conn.set("TOFIELD", c.get('NAME'))
            conn.set("TOINSTANCE", "UPD_CDC" + str(cnt))
            conn.set("TOINSTANCETYPE", "Update Strategy")

    for s in src_attrib:
        if s.get('NAME') != "DTL__CAPXTIMESTAMP":
            conn_sq_trg = etree.SubElement(map, "CONNECTOR")
            conn_sq_trg.set("FROMFIELD", s.get('NAME'))
            conn_sq_trg.set("FROMINSTANCE", sq_name)
            conn_sq_trg.set("FROMINSTANCETYPE", "App Multi-Group Source Qualifier")
            conn_sq_trg.set("TOFIELD", s.get('NAME'))
            conn_sq_trg.set("TOINSTANCE", "UPD_CDC" + str(cnt))
            conn_sq_trg.set("TOINSTANCETYPE", "Update Strategy")

    for s in us_attrib:
        conn_us_cdc = etree.SubElement(map, "CONNECTOR")
        conn_us_cdc.set("FROMFIELD", s.get('NAME'))
        conn_us_cdc.set("FROMINSTANCE", "UPD_CDC" + str(cnt))
        conn_us_cdc.set("FROMINSTANCETYPE", "Update Strategy")
        conn_us_cdc.set("TOFIELD", s.get('NAME').lower())
        conn_us_cdc.set("TOINSTANCE", trg_name_cdc)
        conn_us_cdc.set("TOINSTANCETYPE", "Target Definition")

    conn_us_cdc = etree.SubElement(map, "CONNECTOR")
    conn_us_cdc.set("FROMFIELD", "pk_md5")
    conn_us_cdc.set("FROMINSTANCE", "UPD_CDC" + str(cnt))
    conn_us_cdc.set("FROMINSTANCETYPE", "Update Strategy")
    conn_us_cdc.set("TOFIELD", "pk_md5")
    conn_us_cdc.set("TOINSTANCE", trg_name_cdc)
    conn_us_cdc.set("TOINSTANCETYPE", "Target Definition")


    print(i.get('NAME'))

tree = etree.ElementTree(root)
outfile_name = "m_src_img_sor_pms_combined_cdc" + ".XML"
tree.write(outfile_name, pretty_print=True, encoding="UTF-8", xml_declaration=True,
           doctype="""<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">""")
