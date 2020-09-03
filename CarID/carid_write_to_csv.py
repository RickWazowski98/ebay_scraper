
import csv
import pymongo


def connect_to_db():
    try:
        connection = pymongo.MongoClient('localhost',27017)
        db = connection['slava']
        collection = db['ebay_group_deal_store']
        return collection
    except Exception as err:
        print(err)
        return None


def format_data():
    db = connect_to_db()
    full_data = list(db.find({}, {'Product': 1, 'search_result':1, 'details_search_result':1, 'Images':1, 'info_msg':1, '_id': 0}))
    full_data_dict = {str(item['Product']): item for item in full_data}
    data_to_write = []
    for k in full_data_dict.keys():
        if "info_msg" in full_data_dict[k]:
            if full_data_dict[k]["info_msg"] != "Found 0 products.":
                if "details_search_result" in full_data_dict[k]:
                    item_sku = k
                    try:
                        brand = full_data_dict[k]["details_search_result"][0]["product"].split(" ")[0].replace("\xae","")
                    except (KeyError, IndexError):
                        brand = ""

                    try:
                        item_name = full_data_dict[k]["search_result"][0]["name"]
                    except (KeyError, IndexError):
                        item_name = ""

                    try:
                        part_num = full_data_dict[k]["details_search_result"][0]["part_number"]
                    except (KeyError, IndexError):
                        part_num = ""

                    try:
                        item_type = full_data_dict[k]["search_result"][0]["name"].replace(" ","-").lower()
                    except (KeyError, IndexError):
                        item_type = ""

                    try:
                        price = full_data_dict[k]["details_search_result"][0]["price"].replace("$","")
                    except (KeyError, IndexError):
                        price = ""

                    try:
                        if full_data_dict[k]["Images"] != "#N/A":
                            img = full_data_dict[k]["Images"]
                        elif full_data_dict[k]["details_search_result"][0]["images"] != "":
                            img = full_data_dict[k]["details_search_result"][0]["images"]
                        else:
                            img = ""
                    except (KeyError, IndexError):
                        img = ""

                    try:
                        upc_num = full_data_dict[k]["details_search_result"][0]["upc"]
                    except(KeyError, IndexError):
                        upc_num = ""

                    try:
                        bp1 = full_data_dict[k]["details_search_result"][0]["features"][0]
                    except (KeyError, IndexError):
                        bp1=""

                    try:
                        bp2 = full_data_dict[k]["details_search_result"][0]["features"][1]
                    except (KeyError, IndexError):
                        bp2=""

                    try:
                        bp3 = full_data_dict[k]["details_search_result"][0]["features"][2]
                    except (KeyError, IndexError):
                        bp3 = ""

                    try:
                        bp4 = full_data_dict[k]["details_search_result"][0]["features"][3]
                    except (KeyError, IndexError):
                        bp4 = ""

                    try:
                        bp5 = full_data_dict[k]["details_search_result"][0]["features"][4]
                    except (KeyError, IndexError):
                        bp5 = ""

                    try:
                        prod_descript = "<br><br>DESCRIPTION:<br><br>" + str(full_data_dict[k]["details_search_result"][0]["description"])
                    except (KeyError, IndexError):
                        prod_descript = ""

                    try:
                        oem1 = full_data_dict[k]["details_search_result"][0]["replaces_oe"][0].replace(",","")
                    except (KeyError, IndexError):
                        oem1 = ""

                    try:
                        oem2 = full_data_dict[k]["details_search_result"][0]["replaces_oe"][1].replace(",","")
                    except (KeyError, IndexError):
                        oem2 = ""

                    try:
                        oem3 = full_data_dict[k]["details_search_result"][0]["replaces_oe"][2].replace(",","")
                    except (KeyError, IndexError):
                        oem3 = ""

                    try:
                        oem4 = full_data_dict[k]["details_search_result"][0]["replaces_oe"][3].replace(",","")
                    except (KeyError, IndexError):
                        oem4 = ""

                    try:
                        oem5 = full_data_dict[k]["details_search_result"][0]["replaces_oe"][4].replace(",","")
                    except (KeyError, IndexError):
                        oem5 = ""

                    item = {
                        "feed_product_type":"Autopart",
                        "item_sku":item_sku,
                        "brand_name":brand,
                        "item_name":item_name,
                        "manufacturer": "Sawyer Auto",
                        "part_number":part_num,
                        "item_type":item_type,
                        "fit_type":"Vehicle Specific",
                        "standard_price":price,
                        "quantity":0,
                        "condition_type":"New",
                        "merchant_shipping_group_name":"Migrated Template",
                        "number_of_items":1,
                        "main_image_url":img,
                        "other_image_url1":"",
                        "other_image_url2":"",
                        "other_image_url3":"",
                        "swatch_image_url":"",
                        "parent_child":"",
                        "parent_sku":"",
                        "relationship_type":"",
                        "variation_theme":"",
                        "update_delete":"",
                        "external_product_id":upc_num,
                        "external_product_id_type":"UPC",
                        "product_subtype":"",
                        "product_description":prod_descript,
                        "inner_material_type":"",
                        "outer_material_type":"",
                        "sole_material":"",
                        "model":"",
                        "oe_manufacturer":"None",
                        "department_name":"None",
                        "style_keywords":"",
                        "bullet_point1":bp1,
                        "bullet_point2":bp2,
                        "bullet_point3":bp3,
                        "bullet_point4":bp4,
                        "bullet_point5":bp5,
                        "specific_uses_keywords":"",
                        "target_audience_keywords":"",
                        "thesaurus_attribute_keywords":"",
                        "generic_keywords1":item_type.replace("-"," "),
                        "generic_keywords2":"",
                        "generic_keywords3":"OEM Quality",
                        "generic_keywords4":"",
                        "generic_keywords5":"",
                        "oem_equivalent_part_number1":oem1,
                        "oem_equivalent_part_number2":oem2,
                        "oem_equivalent_part_number3":oem3,
                        "oem_equivalent_part_number4":oem4,
                        "oem_equivalent_part_number5":oem5,
                        "catalog_number":"",
                        "thesaurus_subject_keywords":"",
                        "platinum_keywords1":"",
                        "platinum_keywords2":"",
                        "platinum_keywords3":"",
                        "platinum_keywords4":"",
                        "platinum_keywords5":"",
                        "abpa_partslink_number1":"",
                        "abpa_partslink_number2":"",
                        "abpa_partslink_number3":"",
                        "abpa_partslink_number4":"",
                        "exterior_finish":"Ready To Paint",
                        "color_name":"null",
                        "color_map":"unpainted",
                        "size_name":"",
                        "size_map":"",
                        "amperage_unit_of_measure":"",
                        "amperage":"",
                        "item_length":"",
                        "item_height":"",
                        "item_width":"",
                        "website_shipping_weight_unit_of_measure":"",
                        "website_shipping_weight":"",
                        "item_display_diameter_unit_of_measure":"",
                        "item_diameter_derived":"",
                        "item_display_diameter":"",
                        "item_diameter_unit_of_measure":"",
                        "item_dimensions_unit_of_measure":"",
                        "fulfillment_center_id":"",
                        "package_length":"",
                        "package_dimensions_unit_of_measure":"",
                        "package_weight_unit_of_measure":"",
                        "package_height":"",
                        "package_width":"",
                        "package_weight":"",
                        "legal_disclaimer_description":"",
                        "cpsia_cautionary_statement":"",
                        "cpsia_cautionary_description":"",
                        "import_designation":"",
                        "country_of_origin":"",
                        "item_volume_unit_of_measure":"",
                        "item_volume":"",
                        "external_testing_certification":"",
                        "fabric_type":"",
                        "legal_compliance_certification_metadata":"",
                        "legal_compliance_certification_expiration_date":"",
                        "item_weight_unit_of_measure":"",
                        "item_weight":"",
                        "batteries_required":"",
                        "are_batteries_included":"",
                        "battery_cell_composition":"",
                        "battery_type1":"",
                        "battery_type2":"",
                        "battery_type3":"",
                        "number_of_batteries1":"",
                        "number_of_batteries2":"",
                        "number_of_batteries3":"",
                        "battery_weight":"",
                        "battery_weight_unit_of_measure":"",
                        "number_of_lithium_metal_cells":"",
                        "number_of_lithium_ion_cells":"",
                        "lithium_battery_packaging":"",
                        "lithium_battery_energy_content":"",
                        "lithium_battery_energy_content_unit_of_measure":"",
                        "lithium_battery_weight":"",
                        "lithium_battery_weight_unit_of_measure":"",
                        "supplier_declared_dg_hz_regulation1":"",
                        "supplier_declared_dg_hz_regulation2":"",
                        "supplier_declared_dg_hz_regulation3":"",
                        "supplier_declared_dg_hz_regulation4":"",
                        "supplier_declared_dg_hz_regulation5":"",
                        "hazmat_united_nations_regulatory_id":"",
                        "safety_data_sheet_url":"",
                        "flash_point":"",
                        "ghs_classification_class1":"",
                        "ghs_classification_class2":"",
                        "ghs_classification_class3":"",
                        "california_proposition_65_compliance_type":"",
                        "california_proposition_65_chemical_names1":"",
                        "california_proposition_65_chemical_names2":"",
                        "california_proposition_65_chemical_names3":"",
                        "california_proposition_65_chemical_names4":"",
                        "california_proposition_65_chemical_names5":"",
                        "item_package_quantity":"",
                        "product_tax_code":"",
                        "product_site_launch_date":"",
                        "merchant_release_date":"",
                        "restock_date":"",
                        "map_price":"",
                        "list_price":"",
                        "sale_price":"",
                        "sale_from_date":"",
                        "sale_end_date":"",
                        "condition_note":"",
                        "fulfillment_latency":"",
                        "max_aggregate_ship_quantity":"",
                        "offering_can_be_gift_messaged":"",
                        "offering_can_be_giftwrapped":"",
                        "is_discontinued_by_manufacturer":"",
                        "missing_keyset_reason":"",
                        "offering_end_date":"",
                        "max_order_quantity":"",
                        "offering_start_date":"",
                    }
                    data_to_write.append(item)
    return data_to_write

def write_data_to_csv():
    header = format_data()[0].keys()
    write_data = format_data()
    with open ("Carid.csv", "w", newline="") as file:
        w = csv.writer(file)
        w.writerow(header)
        x = 1
        for item in write_data:
            w.writerow(item.values())
            print("%d - row was write" % x)
            x+=1



def main():
    write_data_to_csv()


if __name__ == "__main__":
    main()
