from lxml import etree
import datetime
import csv
import configparser

# This Python Script Produces one xml for all the mappings combined. The mapping is for IC load from Source to Target.
# This is a modified version of testxml3

# Get Current Date
now = datetime.datetime.now()
now_str = now.strftime("%Y-%m-%d %H:%M:%S")
print(now.strftime("%Y-%m-%d %H:%M:%S"))

# config
sec = "SectionThree"
config = configparser.ConfigParser()
config.sections()
config.read('config.ini')

src_fname = config[sec]['src_fname']
trg_fname = config[sec]['trg_fname']
pkmd5_csv = config[sec]['pkmd5_csv']
repo_name = config[sec]['repo_name']
map_prefix = config[sec]['map_prefix']


# Function to read pkmd5 from csv
def read_pkmd5(table_name):
    fn = "pkmd5_sor_pms.csv"
    with open(fn, "r", encoding="utf8") as f:
        reader = csv.reader(f, delimiter=",", dialect='excel')
        for row in reader:
            if row[0] == table_name:
                return row[1]

def gen_pkmd5(pk_attrib):
    pk_str = "lower(md5("
    for p in range(len(pk_attrib)):
        if p != 0:
            pk_str = pk_str + "||'~'||"
        if pk_attrib[p].get('DATATYPE').lower() == "varchar" or pk_attrib[p].get('DATATYPE').lower() == "char":
            pk_str = pk_str + "rtrim(" + pk_attrib[p].get('NAME').lower() + ")"
        elif pk_attrib[p].get('DATATYPE').lower() == "num32" or pk_attrib[p].get('DATATYPE').lower() == "integer" or pk_attrib[p].get('DATATYPE').lower() == "smallint" or pk_attrib[p].get('DATATYPE').lower() == "decimal":
            pk_str = pk_str + "to_char(" + pk_attrib[p].get('NAME').lower() + ")"
        elif pk_attrib[p].get('DATATYPE').lower() == "date" or pk_attrib[p].get('DATATYPE').lower() == "time" or pk_attrib[p].get('DATATYPE').lower() == "timestamp":
            pk_str = pk_str + "to_char(" + pk_attrib[p].get('NAME').lower() +",'YYYY-MM-DD'" + ")"
    pk_str = pk_str + "))"
    return pk_str


# Function for Powermart Tag
def powermart_tag():
    root = etree.Element("POWERMART")
    root.set("CREATION_DATE", now_str)
    root.set("REPOSITORY_VERSION", "184.93")
    return root


# Function for Repository Tag
def repo_tag():
    repo = etree.SubElement(root, "REPOSITORY")
    repo.set("NAME", repo_name)
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
        doc = etree.parse(trg_fname)
    elif fl_type == "TRF":
        doc = etree.parse('EXP_Ctl_line_no_img_copy.XML')
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
        doc = etree.parse(trg_fname)
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

for i in sources:

    src_name = i.get('NAME')
    trg_name = "sor_" + src_name.lower()

    # SourceField
    src_attrib = field_attr("SRC", src_name)

    # TargetField
    trg_attrib = field_attr("TRG", trg_name)

    # TransformField Tag
    tfm_val = read_attr("TRF", "TRANSFORMFIELD")

    # Mapping Tag
    map_name = map_prefix + src_name.lower()
    map = etree.SubElement(fold, "MAPPING")
    map.set("DESCRIPTION", "")
    map.set("ISVALID", "YES")
    map.set("NAME", map_name)
    map.set("OBJECTVERSION", "1")
    map.set("VERSIONNUMBER", "1")

    # SQ_Transformation
    sq_name = 'SQ_' + src_name
    sq_tfm = etree.SubElement(map, "TRANSFORMATION")
    sq_tfm.set("DESCRIPTION", "")
    sq_tfm.set("NAME", sq_name)
    sq_tfm.set("OBJECTVERSION", "1")
    sq_tfm.set("REUSABLE", "NO")
    sq_tfm.set("TYPE", "Source Qualifier")
    sq_tfm.set("VERSIONNUMBER", "1")

    # SQ_TransformField
    for s in src_attrib:
        sq_tfm_fld = etree.SubElement(sq_tfm, "TRANSFORMFIELD")
        datatype = s.get('DATATYPE')
        precision = s.get('PRECISION')
        scale = s.get('SCALE')
        if (datatype == "varchar" or datatype == "char"):
            datatype = "string"
        elif datatype.lower() == "smallint":
            datatype = "small integer"
        elif datatype.lower() == "date" or datatype.lower() == "time" or datatype.lower() == "timestamp":
            datatype = "date/time"
            precision = "29"
            scale = "9"
        sq_tfm_fld.set("DATATYPE", datatype)
        sq_tfm_fld.set("DEFAULTVALUE", "")
        sq_tfm_fld.set("DESCRIPTION", "")
        sq_tfm_fld.set("NAME", s.get('NAME'))
        sq_tfm_fld.set("PICTURETEXT", s.get('PICTURETEXT'))
        sq_tfm_fld.set("PORTTYPE", "INPUT/OUTPUT")
        sq_tfm_fld.set("PRECISION", precision)
        sq_tfm_fld.set("SCALE", scale)

    # SQ_TableAttribute
    sq_tbl_attr = etree.SubElement(sq_tfm, "TABLEATTRIBUTE")
    sq_tbl_attr.set("NAME", "Sql Query")
    sq_tbl_attr.set("VALUE", "")
    sq_tbl_attr = etree.SubElement(sq_tfm, "TABLEATTRIBUTE")
    sq_tbl_attr.set("NAME", "User Defined Join")
    sq_tbl_attr.set("VALUE", "")
    sq_tbl_attr = etree.SubElement(sq_tfm, "TABLEATTRIBUTE")
    sq_tbl_attr.set("NAME", "Source Filter")
    sq_tbl_attr.set("VALUE", "")
    sq_tbl_attr = etree.SubElement(sq_tfm, "TABLEATTRIBUTE")
    sq_tbl_attr.set("NAME", "Number Of Sorted Ports")
    sq_tbl_attr.set("VALUE", "0")
    sq_tbl_attr = etree.SubElement(sq_tfm, "TABLEATTRIBUTE")
    sq_tbl_attr.set("NAME", "Tracing Level")
    sq_tbl_attr.set("VALUE", "Normal")
    sq_tbl_attr = etree.SubElement(sq_tfm, "TABLEATTRIBUTE")
    sq_tbl_attr.set("NAME", "Select Distinct")
    sq_tbl_attr.set("VALUE", "NO")
    sq_tbl_attr = etree.SubElement(sq_tfm, "TABLEATTRIBUTE")
    sq_tbl_attr.set("NAME", "Is Partitionable")
    sq_tbl_attr.set("VALUE", "NO")
    sq_tbl_attr = etree.SubElement(sq_tfm, "TABLEATTRIBUTE")
    sq_tbl_attr.set("NAME", "Pre SQL")
    sq_tbl_attr.set("VALUE", "")
    sq_tbl_attr = etree.SubElement(sq_tfm, "TABLEATTRIBUTE")
    sq_tbl_attr.set("NAME", "Post SQL")
    sq_tbl_attr.set("VALUE", "")
    sq_tbl_attr = etree.SubElement(sq_tfm, "TABLEATTRIBUTE")
    sq_tbl_attr.set("NAME", "Output is deterministic")
    sq_tbl_attr.set("VALUE", "NO")
    sq_tbl_attr = etree.SubElement(sq_tfm, "TABLEATTRIBUTE")
    sq_tbl_attr.set("NAME", "Output is repeatable")
    sq_tbl_attr.set("VALUE", "Never")

    # GetMd5 Transformation
    getmd5_tfm = etree.SubElement(map, "TRANSFORMATION")
    getmd5_tfm.set("DESCRIPTION", "")
    getmd5_tfm.set("NAME", "EXP_Get_Md5")
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
                    if (datatype == "varchar" or datatype == "char"):
                        datatype = "string"
                    elif datatype.lower() == "smallint":
                        datatype = "small integer"
                    elif datatype.lower() == "date" or datatype.lower() == "time" or datatype.lower() == "timestamp":
                        datatype = "date/time"
                        precision = "29"
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
    getmd5_tfm_fld_pkmd5.set("EXPRESSION", gen_pkmd5(pk_attrib))
    getmd5_tfm_fld_pkmd5.set("EXPRESSIONTYPE", "GENERAL")
    getmd5_tfm_fld_pkmd5.set("NAME", "pk_md5")
    getmd5_tfm_fld_pkmd5.set("PICTURETEXT", "")
    getmd5_tfm_fld_pkmd5.set("PORTTYPE", "OUTPUT")
    getmd5_tfm_fld_pkmd5.set("PRECISION", "32")
    getmd5_tfm_fld_pkmd5.set("SCALE", "0")

    getmd5_tbl_attr_pkmd5 = etree.SubElement(getmd5_tfm, "TABLEATTRIBUTE")
    getmd5_tbl_attr_pkmd5.set("NAME", "Tracing Level")
    getmd5_tbl_attr_pkmd5.set("VALUE", "Normal")

    # Instances
    inst_trg_def = etree.SubElement(map, "INSTANCE")
    inst_trg_def.set("DESCRIPTION", "")
    inst_trg_def.set("NAME", trg_name)
    inst_trg_def.set("TRANSFORMATION_NAME", trg_name)
    inst_trg_def.set("TRANSFORMATION_TYPE", "Target Definition")
    inst_trg_def.set("TYPE", "TARGET")

    inst_src_def = etree.SubElement(map, "INSTANCE")
    inst_src_def.set("DBDNAME", "DB2zOs_IMAGE")
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
    inst_sq.set("TRANSFORMATION_TYPE", "Source Qualifier")
    inst_sq.set("TYPE", "TRANSFORMATION")
    ass_inst_sq = etree.SubElement(inst_sq, "ASSOCIATED_SOURCE_INSTANCE")
    ass_inst_sq.set("NAME", src_name)

    inst_exp_ctl = etree.SubElement(map, "INSTANCE")
    inst_exp_ctl.set("DESCRIPTION", "")
    inst_exp_ctl.set("NAME", "EXP_Ctl_line_no_img_copy")
    inst_exp_ctl.set("REUSABLE", "YES")
    inst_exp_ctl.set("TRANSFORMATION_NAME", "EXP_Ctl_line_no_img_copy")
    inst_exp_ctl.set("TRANSFORMATION_TYPE", "Expression")
    inst_exp_ctl.set("TYPE", "TRANSFORMATION")

    inst_exp_md5 = etree.SubElement(map, "INSTANCE")
    inst_exp_md5.set("DESCRIPTION", "")
    inst_exp_md5.set("NAME", "EXP_Get_Md5")
    inst_exp_md5.set("REUSABLE", "NO")
    inst_exp_md5.set("TRANSFORMATION_NAME", "EXP_Get_Md5")
    inst_exp_md5.set("TRANSFORMATION_TYPE", "Expression")
    inst_exp_md5.set("TYPE", "TRANSFORMATION")

    # connector
    for c in tfm_val:
        if c.get('NAME') != "any":
            conn = etree.SubElement(map, "CONNECTOR")
            conn.set("FROMFIELD", c.get('NAME'))
            conn.set("FROMINSTANCE", "EXP_Ctl_line_no_img_copy")
            conn.set("FROMINSTANCETYPE", "Expression")
            conn.set("TOFIELD", c.get('NAME'))
            conn.set("TOINSTANCE", trg_name)
            conn.set("TOINSTANCETYPE", "Target Definition")

    conn_pkmd5 = etree.SubElement(map, "CONNECTOR")
    conn_pkmd5.set("FROMFIELD", "pk_md5")
    conn_pkmd5.set("FROMINSTANCE", "EXP_Get_Md5")
    conn_pkmd5.set("FROMINSTANCETYPE", "Expression")
    conn_pkmd5.set("TOFIELD", "pk_md5")
    conn_pkmd5.set("TOINSTANCE", trg_name)
    conn_pkmd5.set("TOINSTANCETYPE", "Target Definition")

    for s in src_attrib:
        conn_sq_trg = etree.SubElement(map, "CONNECTOR")
        conn_sq_trg.set("FROMFIELD", s.get('NAME'))
        conn_sq_trg.set("FROMINSTANCE", sq_name)
        conn_sq_trg.set("FROMINSTANCETYPE", "Source Qualifier")
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
        conn_sd_sq.set("TOINSTANCETYPE", "Source Qualifier")

    cFlag = 0
    for s in src_attrib:
        if s.get('DATATYPE') == "varchar" or s.get('DATATYPE') == "char":
            cFlag = 1
            from_fld = s.get('NAME')
            break
    if cFlag == 0:
        from_fld = src_attrib[0].get('NAME')

    conn_sq_ctl = etree.SubElement(map, "CONNECTOR")
    conn_sq_ctl.set("FROMFIELD", from_fld)
    conn_sq_ctl.set("FROMINSTANCE", sq_name)
    conn_sq_ctl.set("FROMINSTANCETYPE", "Source Qualifier")
    conn_sq_ctl.set("TOFIELD", "any")
    conn_sq_ctl.set("TOINSTANCE", "EXP_Ctl_line_no_img_copy")
    conn_sq_ctl.set("TOINSTANCETYPE", "Expression")

    for s in pk_attrib:
        conn_sq_md5 = etree.SubElement(map, "CONNECTOR")
        conn_sq_md5.set("FROMFIELD", s.get('NAME'))
        conn_sq_md5.set("FROMINSTANCE", sq_name)
        conn_sq_md5.set("FROMINSTANCETYPE", "Source Qualifier")
        conn_sq_md5.set("TOFIELD", s.get('NAME'))
        conn_sq_md5.set("TOINSTANCE", "EXP_Get_Md5")
        conn_sq_md5.set("TOINSTANCETYPE", "Expression")

    print(i.get('NAME'))

tree = etree.ElementTree(root)
outfile_name = "m_src_img_sor_gis_combined_wc" + ".XML"
tree.write(outfile_name, pretty_print=True, encoding="UTF-8", xml_declaration=True, doctype="""<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">""")