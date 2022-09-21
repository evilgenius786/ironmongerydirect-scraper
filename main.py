import csv
import json
import os.path
import threading
import traceback

import openpyxl
import requests
from bs4 import BeautifulSoup

brands = [
    'carlislebrass',
    'union',
    'era',
    'yale'
]
api = "https://api.manutantraders.com/product/brands"
headers = {
    'accept': 'application/json, text/plain, */*',
    'ocp-apim-subscription-key': 'cd1817ac421a4eae8bd985e95b139fb1',
    'user-agent': 'Mozilla/5.0',
}
thread_count = 10
semaphore = threading.Semaphore(thread_count)

fields = ["SKU", "Type", "Parent", "Name", "Price", "Categories", "Images",
          "Attribute 1 Name", "Attribute 1 Value(s)", "Attribute 1 Global", "Attribute 1 Visible",
          "Attribute 2 Name", "Attribute 2 Value(s)", "Attribute 2 Global", "Attribute 2 Visible",
          "Description"]
encoding = 'unicode_escape'


def convert(filename):
    wb = openpyxl.Workbook()
    ws = wb.active
    count = 0
    with open(filename, encoding=encoding) as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            ws.append(row)
            count += 1
    if count > 1:
        wb.save(filename.replace("csv", "xlsx"))
    else:
        os.remove(filename)


def processJson():
    # names = []
    products = {}
    for brand in brands:
        products[brand] = {}
        for file in os.listdir(f"{brand}-products"):
            if not file.endswith(".json"):
                continue
            with open(f"./{brand}-products/{file}", 'r') as f:
                try:
                    data = json.load(f)
                    # data['brand'] = brand
                    # print(json.dumps(data, indent=4))
                    try:
                        data['name'] = data['name'].replace('"', '')
                        if data['name'].endswith("Case/Cylinder"):
                            data['features']['Finish'] = data['name'].split("-")[-1].replace(" Case/Cylinder",
                                                                                             "").strip()
                            # print(data['name'])
                            # print(data['features']['Finish'])
                        if "Finish" in data['features'].keys() and data['features']['Finish'] in data['name']:
                            finish = data['features']['Finish']
                        else:
                            with open('Finish.txt', 'r') as f:
                                for fin in f.read().splitlines():
                                    if fin == data['name'].split("-")[-1].strip():
                                        print(f"Found finish ({fin}) in ({data['name']})")
                                        finish = fin
                                        data['features']['Finish'] = fin
                                        break
                        # if finish not in data['name']:
                        #     print(f"Finish ({finish}) not found in ({data['name']})")
                        name = data['name'].replace(f" - {finish}", "").strip()
                        if name not in products[brand].keys():
                            products[brand][name] = []
                        products[brand][name].append(data)
                    except:

                        traceback.print_exc()
                except:
                    traceback.print_exc()
                    print("Error", f"./{brand}-products/{file}")
                    print(f.read())
                    input("Done")

    # print(len(names), names)
    # print(len(set(names)), set(names))
    print(json.dumps(products, indent=4))
    rows = []
    i = 0
    for brand in brands:
        for name, product in products[brand].items():
            print(f"Processing {name}, length ({len(product)})")
            if len(product) == 1:
                row = {
                    "SKU": product[0]['skuCode'],
                    "Type": "Simple",
                    "Parent": "",
                    "Name": product[0]['name'],
                    "Price": product[0]['price'],
                    "Categories": product[0]['attributes']['categories'],
                    "Images": product[0]['imageUrl'],
                    "Attribute 1 Name": "Brand",
                    "Attribute 1 Value(s)": brand,
                    "Attribute 1 Global": "1",
                    "Attribute 1 Visible": "1",
                    "Attribute 2 Name": "Finish",
                    "Attribute 2 Value(s)": product[0]['features']['Finish'] if "Finish" in product[0][
                        'features'].keys() else "",
                    "Attribute 2 Global": "1",
                    "Attribute 2 Visible": "1",
                    "Description": product[0]['description'],
                }
                rows.append(row)
            else:
                p = product[0]
                i += 1
                row = {
                    "SKU": f"{brand}{i:03d}",
                    "Type": "Variable",
                    "Parent": "",
                    "Name": name,
                    "Price": "",
                    "Categories": p['attributes']['categories'],
                    "Images": p['imageUrl'],
                    "Attribute 1 Name": "Brand",
                    "Attribute 1 Value(s)": brand,
                    "Attribute 1 Global": "1",
                    "Attribute 1 Visible": "1",
                    "Attribute 2 Name": "Finish",
                    "Attribute 2 Value(s)": ", ".join([prod['features']['Finish'] for prod in product]) if "Finish" in
                                                                                                           product[0][
                                                                                                               'features'].keys() else "",
                    "Attribute 2 Global": "1",
                    "Attribute 2 Visible": "0",
                    "Description": p['description'],
                }
                rows.append(row)
                for p in product:
                    row = {
                        "SKU": p['skuCode'],
                        "Type": "Variation",
                        "Parent": f"{brand}{i:03d}",
                        "Name": "",
                        "Price": p['price'],
                        "Categories": "",
                        "Images": p['imageUrl'],
                        "Attribute 1 Name": "",
                        "Attribute 1 Value(s)": "",
                        "Attribute 1 Global": "",
                        "Attribute 1 Visible": "",
                        "Attribute 2 Name": "Finish",
                        "Attribute 2 Value(s)": p['features']['Finish'] if "Finish" in p['features'].keys() else "",
                        "Attribute 2 Global": "1",
                        "Attribute 2 Visible": "0",
                        "Description": "",
                    }
                    rows.append(row)
    # print(rows)
    with open("ironmongerydirect.csv", 'w', newline='') as f:
        cfile = csv.DictWriter(f, fieldnames=fields)
        cfile.writeheader()
        cfile.writerows(rows)
    print(f"{len(rows)} rows written to ironmongerydirect.csv")
    convert("ironmongerydirect.csv")


def scrape(brand, page):
    with semaphore:
        params = {'companyID': '1', 'page': page}
        response = requests.get(f'{api}/{brand}', headers=headers, params=params).json()
        # print(json.dumps(response, indent=4))
        with open(f"./{brand}/{page}.json", 'w') as f:
            json.dump(response, f, indent=4)
        print(f"Scraping page {page} of {brand}")


def scrapeListings():
    threads = []
    for brand in brands:
        if not os.path.isdir(brand):
            os.mkdir(brand)
        totalProducts = int(getJson(brand)["view"]["pageInfo"]["totalProducts"])
        print(f"Total products ({brand}):", totalProducts)
        for page in range(1, (totalProducts // 21) + 1):
            if os.path.isfile(f"./{brand}/{page}.json"):
                print(f"Already scraped page {page} of brand {brand}")
            else:
                threads.append(threading.Thread(target=scrape, args=(brand, page,)))
                threads[-1].start()
    for thread in threads:
        thread.join()
    for brand in brands:
        products = []
        for file in os.listdir(brand):
            with open(f"./{brand}/{file}", 'r') as f:
                for product in json.load(f)["view"]["products"]:
                    products.append(product)
        with open(f"{brand}.json", 'w') as f:
            json.dump(products, f, indent=4)
    print("Done")


def getDetails(product, file):
    with semaphore:
        print(f"Scraping product {product['url']}")
        # with open('index.html') as ifile:
        #     content = ifile.read()
        content = requests.get(product['url']).text
        soup = BeautifulSoup(content, 'lxml')
        product.update(
            {
                "description": str(soup.find('ul', {'class': 'product-about__list'})),
                "features": {},
            }
        )
        for div in soup.find_all('div', {"class": "product-specs__main"}):
            try:
                key = div.find("p", {"class": "product-specs__text--name"}).text
                val = div.find("p", {"class": "product-specs__text--value"})
                if val:
                    val = val.text
                else:
                    val = div.find("a")['href']
                product["features"][key] = val
            except:
                traceback.print_exc()
        print(json.dumps(product, indent=4))
        with open(file, 'w') as f:
            json.dump(product, f, indent=4)


def scrapeProducts():
    threads = []
    for brand in brands:
        with open(f"{brand}.json", 'r') as f:
            for product in json.load(f):
                if not os.path.isdir(f"./{brand}-products"):
                    os.mkdir(f"./{brand}-products")
                file = f"./{brand}-products/{product['url'].split('/')[-1]}.json"
                if not os.path.isfile(file):
                    threads.append(threading.Thread(target=getDetails, args=(product, file,)))
                    threads[-1].start()
                else:
                    print(f"Already scraped product {product['url']}")
    for thread in threads:
        thread.join()


def main():
    logo()
    # choice = input("Enter 1 to scrape listings, 2 to scrape products: ")
    choice = "3"
    if choice == "1":
        scrapeListings()
    elif choice == "2":
        scrapeProducts()
    elif choice == "3":
        processJson()
    else:
        print("Invalid choice")
        exit()


def logo():
    print(r"""
 _                                                                            _  _                   _   
(_)                                                                          | |(_)                 | |  
 _  _ __  ___   _ __   _ __ ___    ___   _ __    __ _   ___  _ __  _   _   __| | _  _ __  ___   ___ | |_ 
| || '__|/ _ \ | '_ \ | '_ ` _ \  / _ \ | '_ \  / _` | / _ \| '__|| | | | / _` || || '__|/ _ \ / __|| __|
| || |  | (_) || | | || | | | | || (_) || | | || (_| ||  __/| |   | |_| || (_| || || |  |  __/| (__ | |_ 
|_||_|   \___/ |_| |_||_| |_| |_| \___/ |_| |_| \__, | \___||_|    \__, | \__,_||_||_|   \___| \___| \__|
                                                 __/ |              __/ |                                
                                                |___/              |___/                                 
==========================================================================================================
            ironmongerydirect.co.uk scraper by github.com/evilgenius786
==========================================================================================================
[+] Multithreaded
[+] JSON Output
[+] Super fast!
__________________________________________________________________________________________________________
""")


def getJson(brand):
    return requests.get(f'{api}/{brand}', headers=headers, params={'companyID': '1'}).json()


if __name__ == '__main__':
    main()
