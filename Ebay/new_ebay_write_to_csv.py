
import csv
import pymongo


def connect_to_db():
    try:
        connection = pymongo.MongoClient('localhost',27017)
        db = connection['roma']
        collection = db['ebay_group_deal_store']
        return collection
    except Exception as err:
        print(err)
        return None


def format_data():
    db = connect_to_db()
    full_data = list(db.find({}, {'Product': 1, 'ebay_search_result':1, '_id': 0}))
    full_data_dict = {str(item['Product']): item for item in full_data}
    data_to_write = []
    calc = 1
    for product in full_data_dict.keys():
        print(full_data_dict[product])
        if full_data_dict[product]["ebay_search_result"]["fitment"]:
            product_id = product
            ebay_id = full_data_dict[product]["ebay_search_result"]["ebay_id"]
            calc+=1
        else:
            product_id = product
            ebay_id = ""
        item = {
            "product_id": product_id,
            "ebay_id": ebay_id,
        }
        data_to_write.append(item)
        print(calc)
    return data_to_write


def write_data_to_csv():
    header = format_data()[0].keys()
    write_data = format_data()
    with open ("carid_and_ebay.csv", "w", newline="") as file:
        w = csv.writer(file)
        w.writerow(header)
        x = 1
        for item in write_data:
            w.writerow(item.values())
            #print("%d - row was write" % x)
            x+=1



def main():
    write_data_to_csv()


if __name__ == "__main__":
    main()