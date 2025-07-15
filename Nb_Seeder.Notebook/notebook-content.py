# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

!pip install faker

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import configparser
from dis import show_code
from pickle import PERSID
import random
import datetime
from warnings import catch_warnings
from faker import Faker
import config
from db import create_tables, create_connection, close_connection
import random
from config import Config

# Regions = { 
#   "Asia": ['hi_IN', 'zh_CN', 'ja_JP', 'ko_KR', 'th_TH', 'ru_RU'],
#   "Middle East": ['ar_AA', 'ar_EG', 'ar_JO', 'ar_PS', 'ar_SA'],
#   "Europe": ['da_DK', 'de_AT', 'de_CH', 'de_DE', 'dk_DK', 'el_GR', 'es_ES', 'fr_BE', 'fr_CA', 'fr_CH','fr_FR', 'fr_QC', 'it_CH', 'it_IT', 'nl_BE', 'nl_NL', 'no_NO', 'pl_PL', 'pt_PT', 'ro_RO', 'sk_SK', 'sl_SI', 'sv_SE'],
#   "LatAm": ['es_AR', 'es_BO', 'es_CL', 'es_CO', 'es_CR', 'es_DO', 'es_EC', 'es_ES'],
#   "Africa": ['ar_AA', 'ar_EG', 'ar_JO', 'ar_PS', 'ar_SA', 'tw_GH'],
#   "East Asia": ['zh_CN', 'zh_TW', 'ja_JP', 'ko_KR', 'th_TH'],
#   "East Europe": ['bg_BG', 'cs_CZ', 'uk_UA', 'ru_RU', 'ro_RO', 'pl_PL', 'sk_SK', 'sl_SI']
# }
Regions = {
    "Asia": ['hi_IN', 'zh_CN', 'ja_JP', 'ko_KR', 'th_TH'],
    "Middle East": ['ar_EG', 'ar_SA'],
    "Europe": ['de_DE', 'en_GB', 'es_ES', 'fr_FR', 'it_IT', 'nl_NL', 'pl_PL', 'pt_PT', 'ro_RO', 'ru_RU'],
    "LatAm": ['es_CO', 'es_MX', 'es_ES'],  # ⚠️ Removed es_BO and others not supported
    "Africa": ['en_ZA', 'fr_FR'],  # limited support
    "East Asia": ['zh_CN', 'ja_JP', 'ko_KR'],
    "East Europe": ['cs_CZ', 'pl_PL', 'ro_RO', 'ru_RU', 'uk_UA']
}


if not Config.Region:
    fake = Faker()
else:
    fake = Faker(Regions[Config.Region])

# function to show all locales in Faker


faker_locales = [
        'ar_AA', 'ar_EG', 'ar_JO', 'ar_PS', 'ar_SA', 'bg_BG', 'bs_BA', 'cs_CZ', 'da_DK', 'de_AT', 
        'de_CH', 'de_DE', 'dk_DK', 'el_CY', 'el_GR', 'en_AU', 'en_CA', 'en_GB', 'en_IE', 'en_IN', 
        'en_NZ', 'en_PH', 'en_TH', 'en_US', 'es_AR', 'es_BO', 'es_CL', 'es_CO', 'es_CR', 'es_DO',
        'es_EC', 'es_ES', 'es_GT', 'es_HN', 'es_MX', 'es_NI', 'es_PA', 'es_PE', 'es_PR', 'es_PY', 
        'es_SV', 'es_UY', 'es_VE', 'et_EE', 'fa_IR', 'fi_FI', 'fr_BE', 'fr_CA', 'fr_CH', 'fr_FR', 
        'fr_QC', 'ga_IE', 'he_IL', 'hi_IN', 'hr_HR', 'hu_HU', 'hy_AM', 'id_ID', 'is_IS', 'it_CH', 
        'it_IT', 'ja_JP', 'ka_GE', 'ko_KR', 'lb_LU', 'lt_LT', 'lv_LV', 'mt_MT', 'ne_NP', 'nl_BE', 
        'nl_NL', 'no_NO', 'or_IN', 'pl_PL', 'pt_BR', 'pt_PT', 'ro_RO', 'ru_RU', 'sk_SK', 'sl_SI', 
        'sq_AL', 'sv_SE', 'ta_IN', 'th_TH', 'tr_TR', 'tw_GH', 'uk_UA', 'vi_VN', 'zh_CN', 'zh_TW'
    ]

def test_faker_locales():
    """Test function to show all Faker locales."""
    print("Available Faker locales:")
    #for locale in faker_locales:
    #    show_faker_locale(locale)
    locales = ['es_ES', 'pt_PT', 'pt_BR', 'hi_IN']  #we can define region here 
    # like 
    # Asia: ['hi_IN', 'zh_CN', 'ja_JP', 'ko_KR', 'th_TH', 'ru_RU']
    # Middle_east = ['ar_AA', 'ar_EG', 'ar_JO', 'ar_PS', 'ar_SA']
    # Europe: ['da_DK', 'de_AT', 'de_CH', 'de_DE', 'dk_DK', 'el_GR', 'es_ES', 'fr_BE', 'fr_CA', 'fr_CH','fr_FR', 'fr_QC', 'it_CH', 'it_IT', 'nl_BE', 'nl_NL', 'no_NO', 'pl_PL', 'pt_PT', 'ro_RO', 'sk_SK', 'sl_SI', 'sv_SE']
    # LATAM: ['es_AR', 'es_BO', 'es_CL', 'es_CO', 'es_CR', 'es_DO', 'es_EC', 'es_ES']
    # Africa: ['ar_AA', 'ar_EG', 'ar_JO', 'ar_PS', 'ar_SA', 'tw_GH']
    # East Asia: ['zh_CN', 'zh_TW', 'ja_JP', 'ko_KR', 'th_TH']
    # East Europe: ['bg_BG', 'cs_CZ', 'uk_UA', 'ru_RU', 'ro_RO', 'pl_PL', 'sk_SK', 'sl_SI']

    for _ in range(1,20):
        print("")
        locale = random.sample(locales,1)
        print(f"Locale: {locale}")
        fake = Faker(locale)
        print(f"Example Male Name: {fake.first_name_male()}")
        print(f"Example Female Name: {fake.first_name_female()}")
        print(f"Example Address: {fake.address()}")
        
    return


def test_mimesis():
    """Test function to show all Mimesis locales."""
    from mimesis import Generic
    from mimesis.locales import Locale  # Import the Locale enumeration

    generic = Generic()
    print("Available Mimesis locales:")
    locales = [locale.value for locale in Locale]  # Extract locale values from the enumeration
    print(locales)
    for locale in locales:
        print(locale)
        generic = Generic(locale = locale ,seed = 2) 
        try:
            person = generic.person
            print(f"Example Name: {person.full_name()}")
            addr = generic.address
            print(f"Example Address: {addr.address()}")
            print(f"Example city: {addr.city()}")
            print(f"Example country: {addr.country()}")
            print(f"Example zip: {addr.postal_code()}")
            print(f"Example Email: {person.email()}")
        except Exception as e:
            print(f"Warning for locale {locale}: {e}")
    return

def show_faker_locale(locale):
    """Show Faker locale details."""
    if locale in faker_locales:
        try:
            print("")
            fake = Faker(locale)
            
            print(f"Locale: {locale}")
            print(f"****Locale: {locale}")
            print(f"Example Name: {fake.name()}")

            print(f"Example Male Name: {fake.first_name_male()}")
            print(f"Example Female Name: {fake.first_name_female()}")

            print(f"Example Address: {fake.address()}")
            print(f"Example Email: {fake.email()}")
            try:
                print(f"Example Phone: {fake.phone_number()}")
            except AttributeError:
                print(f"Example Phone: --- not available ---")
        except Exception as e:
            print(f"Warning for locale {locale}: {e}")
        
        # Try different phone number methods
        
    else:
        print(f"Locale '{locale}' not found in Faker locales.")
    return



all_products = []
def cache_all_products():
    global all_products
    if not all_products:
        query = "SELECT id, listPrice FROM products"
        if Config.inStockOnly:
            query += " WHERE listPrice > 0"
        all_products = executeSQL(query)

banks = ["JPMorgan Chase Bank", "Bank of America", "Wells Fargo Bank", "Citibank", "U.S. Bank", "PNC Bank", 
         "Goldman Sachs Bank", "Truist Bank", "Capital One Bank", "TD Bank", "Bank of New York Mellon", 
         "USAA Bank", "Charles Schwab Bank", "American Express Bank", "HSBC Bank USA", "Fifth Third Bank", 
         "Regions Bank", "KeyBank", "Huntington National Bank", "M&T Bank", "Citizens Bank", "BMO Harris Bank", 
         "Ally Bank", "Discover Bank", "Navy Federal Credit Union", "State Farm Bank", "BBVA USA", 
         "SunTrust Bank", "Branch Banking and Trust", "First National Bank", "Santander Bank", 
         "Union Bank", "Comerica Bank", "Zions Bank", "First Citizens Bank", "Associated Bank", "TCF Bank", 
         "Synovus Bank", "BankUnited", "Popular Bank", "Royal Bank of Canada", "Toronto-Dominion Bank", 
         "Bank of Nova Scotia", "Bank of Montreal", "Canadian Imperial Bank of Commerce", "National Bank of Canada", 
         "Desjardins Bank", "Laurentian Bank of Canada", "Canadian Western Bank", "Banco Nacional de Mexico"]


def zipcode_for_state(state_abbr):
    state_zip_ranges = {
    "AL": (35004, 36925),
    "AK": (99501, 99950),
    "AZ": (85001, 86556),
    "AR": (71601, 72959),
    "CA": (90001, 96162),
    "CO": (80001, 81658),
    "CT": (6001, 6389),
    "DE": (19701, 19980),
    "FL": (32004, 34997),
    "GA": (30001, 31999),
    "HI": (96701, 96898),
    "ID": (83201, 83876),
    "IL": (60001, 62999),
    "IN": (46001, 47997),
    "IA": (50001, 52809),
    "KS": (66002, 67954),
    "KY": (40003, 42788),
    "LA": (70001, 71497),
    "ME": (3901, 4992),
    "MD": (20601, 21930),
    "MA": (1001, 2791),
    "MI": (48001, 49971),
    "MN": (55001, 56763),
    "MS": (38601, 39776),
    "MO": (63001, 65899),
    "MT": (59001, 59937),
    "NE": (68001, 69367),
    "NV": (88901, 89883),
    "NH": (3031, 3897),
    "NJ": (7001, 8989),
    "NM": (87001, 88439),
    "NY": (10001, 14925),
    "NC": (27006, 28909),
    "ND": (58001, 58856),
    "OH": (43001, 45999),
    "OK": (73001, 74966),
    "OR": (97001, 97920),
    "PA": (15001, 19640),
    "RI": (2801, 2940),
    "SC": (29001, 29945),
    "SD": (57001, 57799),
    "TN": (37010, 38589),
    "TX": (75001, 88595),
    "UT": (84001, 84791),
    "VT": (5001, 5907),
    "VA": (20101, 24658),
    "WA": (98001, 99403),
    "WV": (24701, 26886),
    "WI": (53001, 54990),
    "WY": (82001, 83414),
    # DC and territories
    "DC": (20001, 20039),
    "AS": (96799, 96799),
    "GU": (96910, 96932),
    "MP": (96950, 96952),
    "PR": (600, 799),
    "VI": (801, 851),
    }
    if state_abbr in state_zip_ranges:
        start, end = state_zip_ranges[state_abbr]
        return f"{random.randint(start, end):05d}"
    else:
        return fake.zipcode()
    
state_area_codes = {
    "AL": [205, 251, 256, 334, 938],
    "AK": [907],
    "AZ": [480, 520, 602, 623, 928],
    "AR": [479, 501, 870],
    "CA": [209, 213, 310, 323, 408, 415, 424, 442, 510, 530, 559, 562, 619, 626, 650, 657, 661, 707, 714, 747, 760, 805, 818, 820, 831, 858, 909, 916, 925, 949, 951],
    "CO": [303, 719, 720, 970],
    "CT": [203, 475, 860, 959],
    "DE": [302],
    "FL": [305, 321, 352, 386, 407, 561, 727, 754, 772, 786, 813, 850, 863, 904, 941, 954],
    "GA": [229, 404, 470, 478, 678, 706, 762, 770, 912],
    "HI": [808],
    "ID": [208, 986],
    "IL": [217, 224, 309, 312, 331, 447, 464, 618, 630, 708, 773, 779, 815, 847, 872],
    "IN": [219, 260, 317, 463, 574, 765, 812, 930],
    "IA": [319, 515, 563, 641, 712],
    "KS": [316, 620, 785, 913],
    "KY": [270, 364, 502, 606, 859],
    "LA": [225, 318, 337, 504, 985],
    "ME": [207],
    "MD": [240, 301, 410, 443, 667],
    "MA": [339, 351, 413, 508, 617, 774, 781, 857, 978],
    "MI": [231, 248, 269, 313, 517, 586, 616, 734, 810, 906, 947, 989],
    "MN": [218, 320, 507, 612, 651, 763, 952],
    "MS": [228, 601, 662, 769],
    "MO": [314, 417, 557, 573, 636, 660, 816, 975],
    "MT": [406],
    "NE": [308, 402, 531],
    "NV": [702, 725, 775],
    "NH": [603],
    "NJ": [201, 551, 609, 732, 848, 856, 862, 908, 973],
    "NM": [505, 575],
    "NY": [212, 315, 332, 347, 516, 518, 585, 607, 631, 646, 680, 716, 718, 838, 845, 914, 917, 929, 934],
    "NC": [252, 336, 704, 743, 828, 910, 919, 980, 984],
    "ND": [701],
    "OH": [216, 220, 234, 283, 326, 330, 380, 419, 440, 513, 567, 614, 740, 937],
    "OK": [405, 539, 580, 918],
    "OR": [458, 503, 541, 971],
    "PA": [215, 223, 267, 272, 412, 445, 484, 570, 582, 610, 717, 724, 814, 835, 878],
    "RI": [401],
    "SC": [803, 839, 843, 854, 864],
    "SD": [605],
    "TN": [423, 615, 629, 731, 865, 901, 931],
    "TX": [210, 214, 254, 281, 325, 346, 361, 409, 430, 432, 469, 512, 682, 713, 726, 737, 806, 817, 830, 832, 903, 915, 936, 940, 945, 956, 972, 979],
    "UT": [385, 435, 801],
    "VT": [802],
    "VA": [276, 434, 540, 571, 703, 757, 804],
    "WA": [206, 253, 360, 425, 509, 564],
    "WV": [304, 681],
    "WI": [262, 274, 414, 534, 608, 715, 920],
    "WY": [307],
    "DC": [202],
    "AS": [684],
    "GU": [671],
    "MP": [670],
    "PR": [787, 939],
    "VI": [340],
}

def phone_prefix_for_state(state_abbr):
    codes = state_area_codes.get(state_abbr)
    if codes:
        return str(random.choice(codes))
    else:
        return str(random.randint(200, 999)) 


def seed_customers(count=10000, batch_size=200):
    # Find the current max customer ID before insertion
    initial_max_ID = executeSQL("SELECT ISNULL(MAX(id), 0) FROM customers")[0][0]
    if initial_max_ID is None:
        initial_max_ID = 0
    inserted = 0
    connection = create_connection()
    cursor = connection.cursor()
    with connection:
        while inserted < count:
            batch = []
            for _ in range(min(batch_size, count - inserted)):
                c = generate_new_customer()
                yr = int(c["dateOfBirth"].split("-")[0])
                mn = int(c["dateOfBirth"].split("-")[1])
                dy = int(c["dateOfBirth"].split("-")[2])
                batch.append((
                    c["firstName"][:50],
                    c["middleName"][:50],
                    c["lastName"][:50],
                    c["company"][:150],
                    datetime.date(yr, mn, dy),
                    c["email"][:50],
                    c["phone"][:50],
                    c["address"][:150],
                    c["city"][:50],
                    c["state"][:2],
                    c["zip"][:10]
                ))
            cursor.executemany(
                """INSERT INTO customers (firstName, middleName, lastName, company, DOB, email, phone, address, city, state, zip) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                batch
            )
            connection.commit()  # Commit the batch insert
            inserted += len(batch)
            print(f"Inserted {inserted} customers...")
            
    # Get all IDs greater than initial_max_ID using the same cursor/connection
    rows = cursor.execute("SELECT id FROM customers WHERE id > ?", (initial_max_ID,)).fetchall()
    new_ids = [row[0] for row in rows]
    cursor.close()
    close_connection(connection)
    print(f"Seeded {count} customers.")
    return new_ids

def get_unpaid_orders(get_first_row = False, date=None):
    if not date:
        date_filter = ""
    else:
        date_filter = f"and orderDate <= '{date}'"
    if get_first_row:
        limit = "TOP 1"
    else:
        limit = ""
    
    strSQL = f"""
        select {limit} o.id, o.orderDate, o.orderSum - coalesce(op.samount, 0) as diff  
        from orders o 
        left join (select orderId, sum(amount) as samount from orderPayments group by orderId) op  
            on o.id = op.orderId 
        where 1 = 1  
            {date_filter}
            and o.orderSum - coalesce(op.samount, 0) >= 0.01
        order by o.orderDate"""
    result = executeSQL(strSQL)
    
    # Convert to list of dictionaries for easier access
    return [{"id": row[0], "orderDate": row[1], "diff": row[2]} for row in result]

def generate_new_customer():
    email = fake.email(safe=False).split('@')[0] + '@' + fake.domain_name(levels=random.randint(1, 2)) 
    fname = fake.first_name()

    is_female = True if random.randint(1, 100) <= 52 else False

    # 52% chance of having female first name, 48% chance of having male
    if is_female:
        # 52% chance of having a middle name
        fname = fake.first_name_female()
    else:
        # 48% chance of having a middle name
        fname = fake.first_name_male()


    if random.randint(0, 3) == 0:
        # 25% chance of no middle name
        mname = ""
    else:
        if is_female:
            mname = fake.first_name_female()
        else:
            mname = fake.first_name_male()
    
    while mname == fname:
        if is_female:
            mname = fake.first_name_female()
        else:
            mname = fake.first_name_male()
    
    lname = fake.last_name()

    address = fake.street_address()  
    city = fake.city()
    state = fake.state_abbr()
    zip_code = zipcode_for_state(state)
    prefix = phone_prefix_for_state(state)
    phone = f"({prefix}){random.randint(100,999)}-{random.randint(1000,9999)}"
    

    return {"firstName": fname, 
            "middleName": mname, 
            "lastName": lname, 
            "dateOfBirth": f"{random.randint(1940, 2007)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}", 
            "company": fake.company(), 
            "email": email, 
            "phone": phone,
            "address": address,
            "city": city,
            "state": state,
            "zip": zip_code}


def executeSQL(strSQL):
    connection = create_connection()
    cursor = connection.cursor()
    cursor.execute(strSQL)
    result = cursor.fetchall()
    cursor.close()
    close_connection(connection)
    return result

def products_for_order(productsCount=2, productQuantityLimit=2):
    cache_all_products()
    selected = random.sample(all_products, random.randint(1, min(productsCount, len(all_products))))
    result = [
        {"product": prod[0], "qty": random.randint(1, productQuantityLimit), "listPrice": prod[1]}
        for prod in selected
    ]
    return result

def generate_order(customer_id=0, productsCount=2, productQuantityLimit=5):
    products = products_for_order(productsCount=productsCount, productQuantityLimit=productQuantityLimit)
    return {"customer_id": customer_id, "products": products}


def seed_orders(customerCount=1000, newCustomersPercentage=Config.CUSTOMERS_PER_DAY_VARIATION_PERCENTAGE, inStockOnly=True, productsCount=2, productQuantityLimit=3, maxOrderCountPerCustomer=3):
    probability_of_multiple_orders = 0.1
    customers = customers_list_for_orders(customerCount, newCustomersPercentage)
    orders = []
    for customer_id in customers:
        orderCount = 0
        while True:
            order = generate_order(customer_id=customer_id, productsCount=2, productQuantityLimit=3)
            orders.append(order)
            orderCount += 1
            if orderCount < maxOrderCountPerCustomer and random.random() < probability_of_multiple_orders:
                continue
            else:
                break
        
    # Assuming you have a function to insert orders into the database
    insert_orders(orders)
    print(f"Seeded {len(orders)} orders.")
    return orders   

def insert_orders(orders):
    connection = create_connection()
    cursor = connection.cursor()
    min_order_id, max_order_id = get_last_order_id(by_date=True)
    doc_order_id = max_order_id - min_order_id 

    yr = int(Config.ORDER_GENERATION_DATE.split("-")[0])
    mn = int(Config.ORDER_GENERATION_DATE.split("-")[1])
    dy = int(Config.ORDER_GENERATION_DATE.split("-")[2])
    
    # Build all order data first
    all_order_data = []
    for order in orders:
        order_total = sum(round(item["listPrice"],2) * item["qty"] for item in order["products"])
        order_discount = 0
        order_data = (
            Config.ORDER_GENERATION_DATE + '/' + str(doc_order_id+1),
            datetime.date(yr, mn, dy),
            order["customer_id"],
            order_discount,
            '{"Created"}',
            order_total,
            ""
        )
        all_order_data.append(order_data)
        doc_order_id += 1
    
    # Process orders in batches of 100
    batch_size = 200
    all_order_ids = []
    
    for i in range(0, len(all_order_data), batch_size):
        batch_order = all_order_data[i:i + batch_size]
        all_params = []
        
        # Flatten parameters for this batch
        for order_data in batch_order:
            all_params.extend(order_data)
        
        # Create placeholders for this batch
        placeholders = ", ".join(["(?, ?, ?, ?, ?, ?, ?)"] * len(batch_order))
        
        sql = f"""
        INSERT INTO orders (orderNumber, orderDate, customerId, discount, statuses, orderSum, notes)
        OUTPUT inserted.id
        VALUES {placeholders}
        """
        
        cursor.execute(sql, all_params)
        batch_order_ids = [row[0] for row in cursor.fetchall()]
        all_order_ids.extend(batch_order_ids)
        
        print(f"Inserted batch {i//batch_size + 1}: {len(batch_order)} orders")
    
    # Now process order items with the actual IDs
    batch_items = []
    for i, order in enumerate(orders):
        order_id = all_order_ids[i]
        for item in order["products"]:
            batch_items.append(
                (
                    order_id,
                    item["product"],
                    item["qty"],
                    round(item["listPrice"],2),
                    round(item["listPrice"],2) * item["qty"],
                    0,
                    round(item["listPrice"],2) * item["qty"],
                    ""
                )
            )

    # Insert order items in batches too
    if batch_items:
        item_batch_size = 200  # Larger batch for simpler inserts
        for i in range(0, len(batch_items), item_batch_size):
            batch = batch_items[i:i + item_batch_size]
            cursor.executemany(
                """INSERT INTO orderItems (order_id, product_id, qty, price, sum, discount, total, notes) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                batch
            )
            print(f"Inserted order items batch {i//item_batch_size + 1}: {len(batch)} items")
        
    connection.commit()
    cursor.close()
    close_connection(connection)
    return all_order_ids

def get_last_order_id(by_date=True):
    if by_date:
        result = executeSQL(f"SELECT MIN(id), MAX(id) FROM orders where orderDate = '{Config.ORDER_GENERATION_DATE}'")
    else:
        result = executeSQL("SELECT MIN(id), MAX(id) FROM orders")
    
    if result and result[0][0] is not None:
        return result[0][0], result[0][1]
    return 0, 0

def customers_list_for_orders(customerCount=10, newPercentage=10):
    customers = executeSQL("SELECT id FROM customers")
    customers_count = min(customerCount, len(customers)) 
    new_customers_count = int(customerCount * newPercentage / 100)
    if customers_count <= 0:
        return []
    if newPercentage > 0:
        if new_customers_count <= customers_count:
            customers_count -= new_customers_count        
    selected_customers = random.sample(customers,  customers_count) 
    if new_customers_count > 0:
        new_customers = seed_customers(new_customers_count)
        print(f"Generated {new_customers_count} new customers.")
        # Wrap each new customer ID as a tuple to match the format of existing customers
        selected_customers.extend([(cid,) for cid in new_customers])
    else:
        print("No new customers generated.")
    return [customer[0] for customer in selected_customers]

def seed_payments(): # not ready yet
    unpaid_orders = get_unpaid_orders(get_first_row=True)
    min_date = unpaid_orders[0]["orderDate"] if unpaid_orders else datetime.date.today()
    if Config.TEST_MODE:
        generate_and_insert_payments(date_from = min_date, date_to = min_date)
    else:
        generate_and_insert_payments(date_from = min_date, date_to = datetime.date.today())
    return True


def generate_and_insert_payments(date_from, date_to):
    paid_percentage = 0.95  # 95% of orders will be paid
    current_date = date_from
    #select 95% of orders where orderDate is equal current_date
    while current_date <= date_to:
        orders =  get_unpaid_orders(date=current_date)
        orders_to_pay = [order for order in orders if random.random() <= paid_percentage]
        insert_payments(orders_to_pay, current_date)
        current_date += datetime.timedelta(days=1)
    # Get final orders for the last date
    orders = get_unpaid_orders(date=date_to)
    insert_payments(orders, date_to)
    return True


def get_record_count(table_name, filter="1 = 1"):
    result = executeSQL(f"SELECT COUNT(*) FROM {table_name} WHERE {filter}")
    if result and result[0][0] is not None:
        return result[0][0]
    return 0

def get_records(table_name, fields = "*", filter="1 = 1"):
    result = executeSQL(f"SELECT {fields} FROM {table_name} WHERE {filter}")
    return result

def seed_inventory():
    stock_products = executeSQL("select p.id from products p left join inventory i on p.id = i.productId where i.id is null and p.listPrice > 0 ")
    warehouses = get_records(table_name="warehouses", fields="id")
    inventory_count = get_record_count(table_name="inventory")
    if not stock_products:
        print("No products with stock found. Skipping inventory seeding.")
        return False
    else:
        print(f"Found {len(stock_products)} products with stock. Seeding inventory...")
        connection = create_connection()
        cursor = connection.cursor()
        batch = []
        for product in stock_products:
            inventory_count += 1
            warehouse = random.sample(warehouses, 1)[0]  # Get one random warehouse
            batch.append((
                product[0],  # product_id
                warehouse[0],  # warehouse_id (first column of the tuple)
                random.randint(5, 1000),  # stock_quantity
                0  # min_qty
                ))
            if len(batch) >= Config.DEFAULT_BATCH_SIZE:
                cursor.executemany(
                    """INSERT INTO inventory (productId, warehouseId, qty, min_qty) 
                        VALUES (?, ?, ?, ?)""",
                    batch
                )
                connection.commit()  # Commit the batch insert
                print(f"Inserted batch of {len(batch)} inventory records.")
                batch = []  # Clear the batch after inserting
        if len(batch)>0:
                cursor.executemany(
                    """INSERT INTO inventory (productId, warehouseId, qty, min_qty) 
                        VALUES (?, ?, ?, ?)""",
                    batch
                )
                connection.commit()  # Commit the batch insert
                print(f"Inserted final batch of {len(batch)} inventory records.")
        cursor.close()
        connection.close()
    return inventory_count



def seed_suppliers():
    supplier_count = get_record_count(table_name="suppliers")
    if supplier_count > 0:
        return supplier_count  # Already seeded
    connection = create_connection()
    cursor = connection.cursor()
    batch = []
    for _ in range(1, Config.SUPPLIERS_TO_CREATE):
        supplier_count += 1
        contact = generate_new_customer()
        batch.append(
            (
                contact["company"],
                contact["address"] + "/n" + contact["city"] + "/n" + contact["state"] + "/n" + contact["zip"] ,
                contact["firstName"] + (" " + contact["middleName"] if contact["middleName"] else "") + " " + contact["lastName"],
                contact["phone"],
                contact["email"],
                random.choice(banks),
                fake.iban()
            )
        )
        if len(batch) >= Config.DEFAULT_BATCH_SIZE:
            cursor.executemany(
                """MERGE suppliers AS target
                USING (VALUES (?, ?, ?, ?, ?, ?, ?)) AS source (companyName, address, contact, phone, email, bank, account)
                ON target.companyName = source.companyName
                WHEN NOT MATCHED THEN
                    INSERT (companyName, address, contact, phone, email, bank, account)
                    VALUES (source.companyName, source.address, source.contact, source.phone, source.email, source.bank, source.account);""",
                batch
            )
            connection.commit()
            batch = []  # Clear the batch after inserting   
    if len(batch)>0:
        cursor.executemany(
            """MERGE suppliers AS target
            USING (VALUES (?, ?, ?, ?, ?, ?, ?)) AS source (companyName, address, contact, phone, email, bank, account)
            ON target.companyName = source.companyName
            WHEN NOT MATCHED THEN
                INSERT (companyName, address, contact, phone, email, bank, account)
                VALUES (source.companyName, source.address, source.contact, source.phone, source.email, source.bank, source.account);""",
            batch
        )

    connection.commit()
    cursor.close()
    connection.close()
    
    return supplier_count

def seed_warehouses():
    warehouse_count = get_record_count(table_name="warehouses")
    if warehouse_count > 0:
        return warehouse_count  # Already seeded
    # Now process order items with the actual IDs
    connecion = create_connection()
    cursor = connecion.cursor()
    batch = []
    for _ in range(1,random.randint(1,5)):        
        warehouse_count += 1
        batch.append(
            (
                fake.first_name(),
                fake.address()
            )
        )
    cursor.executemany(
                """INSERT INTO warehouses (warehouseName, address) 
                    VALUES (?, ?)""",
                batch
            )
    connecion.commit()
    cursor.close()
    connecion.close()
    
    return warehouse_count



def insert_payments(orders_to_pay, payment_date):
    connection = create_connection()
    cursor = connection.cursor()
    batch = []
    for order in orders_to_pay:
        batch.append((
                order["id"],
                payment_date,
                fake.random_element(elements=("Online", "In-Store", "Mobile", "Credit Card")),
                round(order["diff"], 2)
                ))
        if len(batch) >= Config.DEFAULT_BATCH_SIZE:
            cursor.executemany(
                """INSERT INTO orderPayments (orderId, paymentDate, channel, amount) 
                    VALUES (?, ?, ?, ?)""",
                batch
            )
            batch = []  # Clear the batch after inserting
            connection.commit()
    if batch:
        cursor.executemany(
            """INSERT INTO orderPayments (orderId, paymentDate, channel, amount) 
                VALUES (?, ?, ?, ?)""",
            batch
        )
    # Commit any remaining transactions
    connection.commit()
    cursor.close()
    print(f"Seeded {len(orders_to_pay)} transactions.")

    close_connection(connection)

    return True

def seed_purchase_orders(po_count=100000, days_back=720, product_filter="All Sold"):
    """
    Generate purchase orders based on products that appear in orderItems (sold products)
    
    Args:
        po_count: Number of purchase orders to generate
        days_back: Number of days back from today to generate POs
        product_filter: "All Sold" (uses orderItems), "Top Selling", or "Recent Sales"
    """
    supplier_count = get_record_count(table_name="suppliers")
    if supplier_count == 0:
        print("No suppliers found. Please seed suppliers first.")
        return 0
    
    warehouse_count = get_record_count(table_name="warehouses")
    if warehouse_count == 0:
        print("No warehouses found. Please seed warehouses first.")
        return 0
    
    # Build product query based on orderItems
    if product_filter == "All Sold":
        product_query = """
            SELECT DISTINCT p.id, p.title, p.listPrice, 
                   CAST(COALESCE(i.qty, 0) as float) as current_qty, 
                   COALESCE(i.warehouseId, 1) as warehouseId,
                   COALESCE(i.min_qty, 10) as min_qty,
                   SUM(oi.qty) as total_sold,
                   COUNT(DISTINCT oi.order_id) as order_count,
                   AVG(CAST(oi.qty as float)) as avg_qty_per_order,
                   MAX(o.orderDate) as last_sold_date
            FROM products p 
            INNER JOIN orderItems oi ON p.id = oi.product_id
            INNER JOIN orders o ON oi.order_id = o.id
            LEFT JOIN inventory i ON p.id = i.productId 
            GROUP BY p.id, p.title, p.listPrice, i.qty, i.warehouseId, i.min_qty
            ORDER BY total_sold DESC
        """
    elif product_filter == "Top Selling":
        product_query = """
            WITH SalesRanking AS (
                SELECT p.id, p.title, p.listPrice, 
                       CAST(COALESCE(i.qty, 0) as float) as current_qty, 
                       COALESCE(i.warehouseId, 1) as warehouseId,
                       COALESCE(i.min_qty, 10) as min_qty,
                       SUM(oi.qty) as total_sold,
                       COUNT(DISTINCT oi.order_id) as order_count,
                       AVG(CAST(oi.qty as float)) as avg_qty_per_order,
                       MAX(o.orderDate) as last_sold_date,
                       PERCENT_RANK() OVER (ORDER BY SUM(oi.qty) DESC) as sales_rank
                FROM products p 
                INNER JOIN orderItems oi ON p.id = oi.product_id
                INNER JOIN orders o ON oi.order_id = o.id
                LEFT JOIN inventory i ON p.id = i.productId 
                GROUP BY p.id, p.title, p.listPrice, i.qty, i.warehouseId, i.min_qty
            )
            SELECT id, title, listPrice, current_qty, warehouseId, min_qty, 
                   total_sold, order_count, avg_qty_per_order, last_sold_date
            FROM SalesRanking 
            WHERE sales_rank <= 0.5
            ORDER BY total_sold DESC
        """
    elif product_filter == "Recent Sales":
        product_query = """
            SELECT DISTINCT p.id, p.title, p.listPrice, 
                   CAST(COALESCE(i.qty, 0) as float) as current_qty, 
                   COALESCE(i.warehouseId, 1) as warehouseId,
                   COALESCE(i.min_qty, 10) as min_qty,
                   SUM(oi.qty) as total_sold,
                   COUNT(DISTINCT oi.order_id) as order_count,
                   AVG(CAST(oi.qty as float)) as avg_qty_per_order,
                   MAX(o.orderDate) as last_sold_date
            FROM products p 
            INNER JOIN orderItems oi ON p.id = oi.product_id
            INNER JOIN orders o ON oi.order_id = o.id
            LEFT JOIN inventory i ON p.id = i.productId 
            WHERE o.orderDate >= DATEADD(day, -30, GETDATE())
            GROUP BY p.id, p.title, p.listPrice, i.qty, i.warehouseId, i.min_qty
            ORDER BY total_sold DESC
        """
    else:
        print(f"Invalid product_filter: {product_filter}. Use 'All Sold', 'Top Selling', or 'Recent Sales'")
        return 0
    
    products = executeSQL(product_query)
    
    if not products:
        print(f"No products found in orderItems for filter '{product_filter}'. Please ensure orders have been seeded.")
        return 0
    
    print(f"Found {len(products)} sold products for PO generation with filter '{product_filter}'")
    
    suppliers = get_records(table_name="suppliers", fields="id, companyName")
    warehouses = get_records(table_name="warehouses", fields="id, warehouseName")
    
    connection = create_connection()
    cursor = connection.cursor()
    
    po_generated = 0
    start_date = datetime.date.today() - datetime.timedelta(days=days_back)
    all_po_data = []
    
    for _ in range(po_count):
        # Generate PO data
        random_days = random.randint(0, days_back)
        po_date = start_date + datetime.timedelta(days=random_days)
        supplier = random.choice(suppliers)
        warehouse = random.choice(warehouses)
        po_number = f"PO-{po_date.strftime('%Y%m%d')}-{random.randint(1000, 9999)}-{po_generated + 1:06d}"
        
        # Select products based on filter
        if product_filter == "Top Selling":
            top_sellers = products[:len(products)//2]
            other_products = products[len(products)//2:]
            num_top = random.randint(2, min(8, len(top_sellers)))
            num_other = random.randint(0, min(3, len(other_products)))
            selected_products = random.sample(top_sellers, num_top)
            if num_other > 0 and other_products:
                selected_products.extend(random.sample(other_products, num_other))
        elif product_filter == "Recent Sales":
            recent_products = [p for p in products if p[9]]
            num_products = random.randint(3, min(10, len(recent_products)))
            selected_products = random.sample(recent_products, num_products)
        else:  # "All Sold"
            num_products = random.randint(4, min(12, len(products)))
            selected_products = random.sample(products, num_products)
        
        po_total = 0
        po_items_for_this_po = []
        
        for product in selected_products:
            # Make sure product has all required elements before unpacking
            if len(product) >= 8:
                product_id, product_title, list_price, current_qty = product[0], product[1], product[2], product[3]
                total_sold, order_count = product[6], product[7]
                
                # Calculate order quantity
                monthly_demand = max(int(total_sold / 3), 5)
                if total_sold > 100:
                    order_qty = monthly_demand * random.randint(2, 4)
                elif total_sold > 25:
                    order_qty = monthly_demand * random.randint(3, 6)
                else:
                    order_qty = max(monthly_demand * random.randint(6, 12), 20)
                
                net_order_qty = max(order_qty - current_qty, 10)
                if order_count > 20:
                    net_order_qty = int(net_order_qty * 1.3)
                
                # Calculate pricing
                if list_price > 0:
                    base_ratio = random.uniform(0.5, 0.75)
                    if net_order_qty > 500:
                        volume_discount = 0.10
                    elif net_order_qty > 200:
                        volume_discount = 0.07
                    elif net_order_qty > 100:
                        volume_discount = 0.05
                    else:
                        volume_discount = 0.02
                    
                    velocity_discount = min(total_sold / 1000, 0.05)
                    final_ratio = base_ratio * (1 - volume_discount - velocity_discount)
                    unit_price = round(list_price * final_ratio, 2)
                else:
                    unit_price = round(random.uniform(10, 150), 2)
                
                line_total = round(unit_price * float(net_order_qty), 2)
                
                po_items_for_this_po.append({
                    'product_id': product_id,
                    'qty': net_order_qty,
                    'unit_price': unit_price,
                    'line_total': line_total,
                    'product_title': product_title,
                    'total_sold': total_sold,
                    'current_inventory': current_qty,
                    'monthly_demand': monthly_demand
                })
                
                po_total += line_total
            else:
                # Skip products with incomplete data
                continue
        
        # Calculate additional costs
        shipping_cost = max(round(po_total * random.uniform(0.015, 0.04), 2), 20.0)
        tax_amount = round(po_total * random.uniform(0.05, 0.095), 2)
        discount = 0
        if po_total > 15000:
            discount = round(po_total * random.uniform(0.04, 0.08), 2)
        elif po_total > 5000:
            discount = round(po_total * random.uniform(0.02, 0.04), 2)
        
        total_amount = round(po_total + shipping_cost + tax_amount - discount, 2)
        requested_delivery = po_date + datetime.timedelta(days=random.randint(7, 25))
        
        # Determine status
        days_since_po = (datetime.date.today() - po_date).days
        has_urgent_items = any(item['current_inventory'] <= 10 for item in po_items_for_this_po)
        
        if days_since_po > 30:
            status = "Received"
            actual_delivery = requested_delivery + datetime.timedelta(days=random.randint(-2, 8))
        elif days_since_po > 14:
            status = "Received" if has_urgent_items else random.choice(["Received", "In-Transit"])
            actual_delivery = requested_delivery + datetime.timedelta(days=random.randint(-5, 3)) if status == "Received" else None
        else:
            status_weights = [0.2, 0.5, 0.3] if has_urgent_items else [0.1, 0.4, 0.5]
            status = random.choices(["Received", "In-Transit", "Pending"], weights=status_weights)[0]
            actual_delivery = requested_delivery + datetime.timedelta(days=random.randint(-3, 4)) if status == "Received" else None
        
        # Generate notes
        high_velocity_items = [item for item in po_items_for_this_po if item['total_sold'] > 50]
        low_stock_items = [item for item in po_items_for_this_po if item['current_inventory'] <= 10]
        notes_parts = [f"Restock order for {len(po_items_for_this_po)} sold products"]
        if high_velocity_items:
            notes_parts.append(f"{len(high_velocity_items)} high-velocity items")
        if low_stock_items:
            notes_parts.append(f"{len(low_stock_items)} low-stock items")
        notes = ". ".join(notes_parts) + f". Based on sales data from orderItems."
        
        # PO header data
        actual_delivery_param = actual_delivery if actual_delivery else None
        approved_by_param = "System" if status != "Pending" else None
        approved_date_param = datetime.datetime.now() if status != "Pending" else None

        po_data = (
            po_number, po_date, supplier[0], warehouse[0],
            requested_delivery, actual_delivery_param, discount, shipping_cost, tax_amount, total_amount,
            status, notes, datetime.datetime.now(), datetime.datetime.now(),
            "System", approved_by_param, approved_date_param
        )
        
        all_po_data.append((po_data, po_items_for_this_po))
        po_generated += 1
    
    # Insert POs in smaller batches to avoid SQL Server parameter limit
    inserted_pos = 0
    # Calculate max batch size: 2100 parameters � 17 fields = ~123 records
    # Use 100 to be safe
    batch_size = 100  # Changed from 200 to 100
    
    for i in range(0, len(all_po_data), batch_size):
        batch_po_data = all_po_data[i:i + batch_size]
        
        # Prepare PO headers for batch insert
        po_header_batch = [po_data for po_data, _ in batch_po_data]
        
        # Replace the current PO header insertion with this approach
        all_params = []
        
        # Flatten parameters for this batch
        for po_data in po_header_batch:
            all_params.extend(po_data)

        # Create placeholders for this batch
        placeholders = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"] * len(po_header_batch))

        sql = f"""
        INSERT INTO purchaseOrders (poNumber, poDate, supplierId, warehouseId, requestedDeliveryDate, 
                                    actualDeliveryDate, discount, shippingCost, taxAmount, totalAmount, 
                                    status, notes, createdDate, modifiedDate, createdBy, approvedBy, approvedDate)
        OUTPUT inserted.id
        VALUES {placeholders}
        """

        cursor.execute(sql, all_params)
        po_ids = [row[0] for row in cursor.fetchall()]

        # If fewer orders were inserted than expected, log a warning
        if len(po_ids) < len(batch_po_data):
            print(f"Warning: Only {len(po_ids)} out of {len(batch_po_data)} purchase orders were inserted.")

        # Prepare all PO items for this batch
        all_po_items_batch = []
        for po_idx, (po_data, po_items) in enumerate(batch_po_data):
            if po_idx < len(po_ids):  # Only process POs that were successfully inserted
                po_id = po_ids[po_idx]
                po_status = po_data[10]  # status field
                
                for item in po_items:
                    # Calculate received quantity based on status
                    if po_status == "Received":
                        received_qty = random.randint(int(item['qty'] * 0.95), item['qty'])
                    elif po_status == "In-Transit":
                        received_qty = random.randint(0, int(item['qty'] * 0.2))
                    else:
                        received_qty = 0
                    
                    all_po_items_batch.append((
                        po_id, item['product_id'], item['qty'], item['unit_price'], item['line_total'],
                        0, received_qty, 
                        f"Sold: {item['total_sold']}, Monthly demand: {item['monthly_demand']}, Stock: {item['current_inventory']}"
                    ))
        
        # Insert PO items in sub-batches of 200 if needed
        if all_po_items_batch:
            item_batch_size = 500
            for j in range(0, len(all_po_items_batch), item_batch_size):
                item_batch = all_po_items_batch[j:j + item_batch_size]
                cursor.executemany(
                    """INSERT INTO purchaseOrderItems (purchaseOrderId, productId, qty, unitPrice, lineTotal, discount, receivedQty, notes) 
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", 
                    item_batch
                )
        
        connection.commit()
        inserted_pos += len(po_header_batch)
        print(f"Inserted {inserted_pos} purchase orders...")
    
    cursor.close()
    close_connection(connection)
    print(f"Generated {po_generated} purchase orders based on orderItems using '{product_filter}' filter.")
    return po_generated

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# # ----------------------------------
# # Fabric-Compatible Data Seeder Notebook
# # ----------------------------------

# import random
# import datetime
# from faker import Faker
# from sqlalchemy import create_engine

# # ---------------------------
# # Configuration Class
# # ---------------------------
# class Config:
#     inStockOnly = True
#     CUSTOMERS_PER_DAY_VARIATION_PERCENTAGE = 10
#     ORDER_GENERATION_DATE = "2024-01-01"
#     TEST_MODE = False
#     DEFAULT_BATCH_SIZE = 1000
#     CUSTOMERS_TO_CREATE = 100000
#     CUSTOMERS_PER_DAY = 1000
#     MAX_PRODUCTS_PER_ORDER = 5
#     MAX_ORDERS_PER_CUSTOMER = 2
#     ITERATIONS_IN_TEST_MODE = 10
#     SQL_SERVER_LOCAL = False

# # ---------------------------
# # SQL Server Connection
# # ---------------------------
# USE_LOCAL_DB = False

# def create_connection():
#     if USE_LOCAL_DB or Config.SQL_SERVER_LOCAL:
#         conn_str = (
#             "mssql+pyodbc://localhost/test?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
#         )
#     else:
#         conn_str = (
#             "mssql+pyodbc://celestial-sa:srGnyE%g8(95)@dataandai-celestial.database.windows.net/"
#             "Test_Mirror_Fabric_Database?driver=ODBC+Driver+17+for+SQL+Server"
#         )
#     return create_engine(conn_str).raw_connection()

# def close_connection(conn):
#     if conn:
#         conn.close()

# # ---------------------------
# # Execute SQL Utility
# # ---------------------------
# def executeSQL(sql):
#     conn = create_connection()
#     cursor = conn.cursor()
#     cursor.execute(sql)
#     result = cursor.fetchall()
#     cursor.close()
#     close_connection(conn)
#     return result

# # ---------------------------
# # ZIP Code by State Abbreviation
# # ---------------------------
# def zipcode_for_state(state_abbr):
#     state_zip_ranges = {
#         "AL": (35004, 36925), "AK": (99501, 99950), "AZ": (85001, 86556), "AR": (71601, 72959),
#         "CA": (90001, 96162), "CO": (80001, 81658), "CT": (6001, 6389), "DE": (19701, 19980),
#         "FL": (32004, 34997), "GA": (30001, 31999), "HI": (96701, 96898), "ID": (83201, 83876),
#         "IL": (60001, 62999), "IN": (46001, 47997), "IA": (50001, 52809), "KS": (66002, 67954),
#         "KY": (40003, 42788), "LA": (70001, 71497), "ME": (3901, 4992), "MD": (20601, 21930),
#         "MA": (1001, 2791), "MI": (48001, 49971), "MN": (55001, 56763), "MS": (38601, 39776),
#         "MO": (63001, 65899), "MT": (59001, 59937), "NE": (68001, 69367), "NV": (88901, 89883),
#         "NH": (3031, 3897), "NJ": (7001, 8989), "NM": (87001, 88439), "NY": (10001, 14925),
#         "NC": (27006, 28909), "ND": (58001, 58856), "OH": (43001, 45999), "OK": (73001, 74966),
#         "OR": (97001, 97920), "PA": (15001, 19640), "RI": (2801, 2940), "SC": (29001, 29945),
#         "SD": (57001, 57799), "TN": (37010, 38589), "TX": (75001, 88595), "UT": (84001, 84791),
#         "VT": (5001, 5907), "VA": (20101, 24658), "WA": (98001, 99403), "WV": (24701, 26886),
#         "WI": (53001, 54990), "WY": (82001, 83414), "DC": (20001, 20039), "AS": (96799, 96799),
#         "GU": (96910, 96932), "MP": (96950, 96952), "PR": (600, 799), "VI": (801, 851),
#     }
#     if state_abbr in state_zip_ranges:
#         start, end = state_zip_ranges[state_abbr]
#         return f"{random.randint(start, end):05d}"
#     else:
#         return fake.zipcode()

# # ---------------------------
# # Seeder Logic with Details
# # ---------------------------
# fake = Faker()

# def generate_new_customer():
#     email = fake.email()
#     fname = fake.first_name()
#     mname = fake.first_name() if random.random() > 0.25 else None
#     lname = fake.last_name()
#     address = fake.street_address()
#     city = fake.city()
#     state = fake.state_abbr()
#     zip_code = zipcode_for_state(state)
#     phone = fake.phone_number()
#     dob = fake.date_of_birth(minimum_age=18, maximum_age=80)

#     return (fname, mname, lname, fake.company(), dob, email, phone, address, city, state, zip_code)

# def seed_customers(count=10000, batch_size=200):
#     print(f"Seeding {count} customers (batch size: {batch_size})")
#     conn = create_connection()
#     cursor = conn.cursor()
#     inserted = 0

#     while inserted < count:
#         batch = []
#         for _ in range(min(batch_size, count - inserted)):
#             batch.append(generate_new_customer())

#         cursor.executemany("""
#             INSERT INTO Customers (firstName, middleName, lastName, company, DOB, email, phone, address, city, state, zip)
#             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", batch)

#         conn.commit()
#         inserted += len(batch)
#         print(f"Inserted {inserted} customers...")

#     cursor.close()
#     close_connection(conn)
#     print("✅ Customers seeded.")

# def seed_orders(customer_count, days=1, insert_only=True, max_products=5, warehouse_count=1, max_orders_per_customer=2):
#     print("Seeding orders...")
#     conn = create_connection()
#     cursor = conn.cursor()

#     customer_ids = [row[0] for row in executeSQL("SELECT id FROM Customers")]
#     product_list = executeSQL("SELECT id, listPrice FROM Products" + (" WHERE listPrice > 0" if Config.inStockOnly else ""))
#     order_date = datetime.datetime.strptime(Config.ORDER_GENERATION_DATE, "%Y-%m-%d").date()

#     for _ in range(customer_count):
#         customer_id = random.choice(customer_ids)
#         order_count = random.randint(1, max_orders_per_customer)

#         for _ in range(order_count):
#             order_number = f"ORD-{random.randint(100000, 999999)}"
#             discount = round(random.uniform(0, 10), 2)
#             status = random.choice(["Created", "Confirmed", "Shipped"])
#             notes = fake.sentence()

#             order_sum = 0.0
#             cursor.execute("""
#                 INSERT INTO Orders (orderNumber, orderDate, customerId, discount, statuses, orderSum, notes)
#                 VALUES (?, ?, ?, ?, ?, ?, ?)""",
#                 (order_number, order_date, customer_id, discount, status, order_sum, notes))

#             order_id = cursor.execute("SELECT @@IDENTITY").fetchval()

#             items = []
#             for _ in range(random.randint(1, max_products)):
#                 prod_id, list_price = random.choice(product_list)
#                 qty = random.randint(1, 5)
#                 total = round(qty * list_price, 2)
#                 order_sum += total
#                 items.append((order_id, prod_id, qty, list_price, total, 0, total, fake.word()))

#             cursor.executemany("""
#                 INSERT INTO OrderItems (order_id, product_id, qty, price, sum, discount, total, notes)
#                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", items)

#             cursor.execute("UPDATE Orders SET orderSum = ? WHERE id = ?", (order_sum, order_id))

#         order_date += datetime.timedelta(days=1)

#     conn.commit()
#     cursor.close()
#     close_connection(conn)
#     print("✅ Orders seeded.")

# def seed_payments():
#     print("Seeding payments...")
#     conn = create_connection()
#     cursor = conn.cursor()

#     unpaid_orders = executeSQL("""
#         SELECT o.id, o.orderSum - COALESCE(p.samount, 0) AS due
#         FROM Orders o
#         LEFT JOIN (
#             SELECT orderId, SUM(amount) AS samount FROM OrderPayments GROUP BY orderId
#         ) p ON o.id = p.orderId
#         WHERE o.orderSum - COALESCE(p.samount, 0) > 0.01
#         ORDER BY o.orderDate
#     """)

#     paid_percentage = 0.95
#     payment_channels = ["Online", "In-Store", "Mobile", "Credit Card"]
#     payment_date = datetime.date.today()

#     payments = []
#     for order_id, due in unpaid_orders:
#         if random.random() <= paid_percentage:
#             payments.append((order_id, payment_date, random.choice(payment_channels), round(due, 2)))

#     if payments:
#         cursor.executemany("""
#             INSERT INTO OrderPayments (orderId, paymentDate, channel, amount)
#             VALUES (?, ?, ?, ?)""", payments)

#     conn.commit()
#     cursor.close()
#     close_connection(conn)
#     print(f"✅ Seeded {len(payments)} payments.")

#     cursor.execute("""
#         IF OBJECT_ID('orderItems', 'U') IS NULL
#         CREATE TABLE orderItems (
#             id BIGINT IDENTITY(1,1) PRIMARY KEY,
#             order_id BIGINT,
#             product_id BIGINT,
#             qty INT,
#             price FLOAT,
#             sum FLOAT,
#             discount FLOAT,
#             total FLOAT,
#             notes VARCHAR(250)
#         )
#     """)

#     cursor.execute("""
#         IF OBJECT_ID('orderPayments', 'U') IS NULL
#         CREATE TABLE orderPayments (
#             id INT IDENTITY(1,1) PRIMARY KEY,
#             product_id INT NOT NULL FOREIGN KEY REFERENCES products(id),
#             customer_id INT NOT NULL FOREIGN KEY REFERENCES customers(id),
#             quantity INT NOT NULL
#         )
#     """)

#     cursor.execute("""
#         IF OBJECT_ID('warehouses', 'U') IS NULL
#         CREATE TABLE warehouses (
#             id BIGINT IDENTITY(1,1) PRIMARY KEY,
#             warehouse_name NVARCHAR(100),
#             address NVARCHAR(250)
#         )
#     """)

#     cursor.execute("""
#         IF OBJECT_ID('inventory', 'U') IS NULL
#         CREATE TABLE inventory (
#             id BIGINT IDENTITY(1,1) PRIMARY KEY,
#             productId BIGINT,
#             warehouseId BIGINT,
#             qty DECIMAL(18, 6),
#             min_qty DECIMAL(18, 6),
#             CONSTRAINT UK_inventory_productId_warehouseId UNIQUE(productId, warehouseId)
#         )
#     """)

#     connection.commit()
#     cursor.close()
#     print("✅ All tables created successfully.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
