import requests
import json
import pandas as pd
import argparse
import datetime
import csv
import re
from bs4 import BeautifulSoup
from lxml import etree


class RiteaidReviewScraper:

    def __init__(self, input_file_path):
        self.input_file_path = input_file_path
        rawlist = self._read_csv(input_file_path)
        self._riteaid(rawlist)

    def _get_parameters(self):
        payload = {}
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:106.0) Gecko/20100101 Firefox/106.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Origin': 'https://www.riteaid.com',
            'Connection': 'keep-alive',
            'Referer': 'https://www.riteaid.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site'
        }
        return headers, payload

    def get_sku_url(self, product_id):
        sku_url = "https://api.bazaarvoice.com/data/products.json?passkey=cap72Fd4Mme89q1eAHJ9FSMuCzWo7B2iyEMKQNJatkIQk&locale=en_US&allowMissing=true&apiVersion=5.4&filter=id:" + str(
            product_id)
        return sku_url

    def date_filter(self,date):
        x = date[0:10].replace("-", " ")
        d1 = datetime.datetime.strptime(x, "%Y %m %d")
        d2 = datetime.datetime(2022, 11, 1)
        d3 = datetime.datetime(2022, 12, 31)
        if (d2 < d1 < d3) :
            return True
        else:
            return False

    def get_sku_json(self, sku_url):
        sku_url = self.get_sku_url(sku_url)
        headers, payload = self._get_parameters()
        sku_json = requests.request("GET", sku_url, headers=headers, data=payload)
        sku_json = sku_json.json()
        return sku_json

    def get_sku_data(self, sku_json):
        sku_json = self.get_sku_json(sku_json)
        sku_brand_details = []
        SKU = sku_json["Results"][0]["Name"]
        Brand = sku_json["Results"][0]["Brand"]["Name"]
        Root = sku_json["Results"][0]["ProductPageUrl"]
        basic_details = {
            "SKU": SKU,
            "Brand": Brand,
            "Root": Root
        }
        sku_brand_details.append(basic_details)
        return sku_brand_details

    def get_product_rating_url(self, product_id):
        product_rating_url = "https://api.bazaarvoice.com/data/display/0.2alpha/product/summary?PassKey=cap72Fd4Mme89q1eAHJ9FSMuCzWo7B2iyEMKQNJatkIQk&productid=" + str(
            product_id) + "&contentType=reviews,questions&reviewDistribution=primaryRating,recommended&rev=0&contentlocale=en*,en_US"
        return product_rating_url

    def get_product_rating_json(self, product_rating_url):
        product_rating_url = self.get_product_rating_url(product_rating_url)
        headers, payload = self._get_parameters()
        product_rating_json = requests.request("GET", product_rating_url, headers=headers, data=payload)
        product_rating_json = product_rating_json.json()
        return product_rating_json

    def get_product_rating_data(self, product_rating_json):
        product_rating_data = self.get_product_rating_json(product_rating_json)
        RatingCount = product_rating_data["reviewSummary"]["numReviews"]
        Product_Rating = product_rating_data["reviewSummary"]["primaryRating"]["average"]
        return RatingCount, Product_Rating

    def get_product_rating(self, product_rating_json):
        Product_Rating_details = []
        RatingCount, Product_Rating = self.get_product_rating_data(product_rating_json)
        Product_Rating_details_ = {
            "RatingCount": RatingCount,
            "Product_Rating": Product_Rating
        }
        Product_Rating_details.append(Product_Rating_details_)
        return Product_Rating_details

    def get_page_count(self, product_rating_json):
        RatingCount, Product_Rating = self.get_product_rating_data(product_rating_json)
        Page_Count = ((RatingCount - 8) // 30) + 3
        return Page_Count

    def generate_reviews(self, product_id):
        Page_Count = self.get_page_count(product_id)
        urls = self.get_reviews_urls(Page_Count, product_id)
        final_review_details = []
        for url in urls:
            review_json = self.get_reviews_json(url)
            review_details = self.get_reviews(review_json, product_id)
            final_review_details.extend(review_details)
        return final_review_details

    def get_reviews_urls(self,Page_Count, product_id):
        urls = []
        Page_Count = self.get_page_count(product_id)
        q0, q1, q2, i = 0, 8, 30, 0
        while i < Page_Count:
            url = self.get_reviews_url(product_id, q0, q1)
            review_json = self.get_reviews_json(url)
            date = self.get_last_review_date(review_json, product_id)
            print(date)
            is_in_daterange = self.date_filter(date)
            if is_in_daterange == True:
                urls.append(url)
                q0, q1, i = q2, q1 + q2, i + 1
            else:
                exit()
        return urls

    def get_reviews_url(self,product_id, q0, q1):
        review_url = "https://api.bazaarvoice.com/data/batch.json?resource.q0=reviews&filter.q0=productid%3Aeq%3A" + str(
            product_id) + "&filter.q0=contentlocale%3Aeq%3Aen*%2Cen_US%2Cen_US&filter.q0=isratingsonly%3Aeq%3Afalse&filter_reviews.q0=contentlocale%3Aeq%3Aen*%2Cen_US%2Cen_US&include.q0=authors%2Cproducts&filteredstats.q0=reviews&limit.q0=" + str(
            q1) + "&offset.q0=" + str(
            q0) + "&sort.q0=submissiontime%3Adesc&passkey=cap72Fd4Mme89q1eAHJ9FSMuCzWo7B2iyEMKQNJatkIQk&apiversion=5.5&displaycode=28398-en_us"
        return review_url

    def get_reviews_json(self, review_url):
        headers, payload = self._get_parameters()
        review_json = requests.request("GET", review_url, headers=headers, data=payload)
        review_json = review_json.json()
        return review_json

    def get_last_review_date(self, review_json, product_id):
        date_elements = review_json["BatchedResults"]["q0"]["Results"]
        for date_element in date_elements:
            date = date_element["SubmissionTime"]
        return date

    def get_reviews(self, review_json, product_id):
        Elements = review_json["BatchedResults"]["q0"]["Results"]
        ReviewCount = review_json["BatchedResults"]["q0"]["TotalResults"]
        review_details = []
        for element in Elements:
            Review_Rating = element["Rating"]
            Location = element["UserLocation"]
            Review = element["ReviewText"]
            date = element["SubmissionTime"]
            #is_in_daterange = self.date_filter(date)
            Title = element["Title"]
            Author = element["UserNickname"]
            review_details_raw = {
                "Review Rating": Review_Rating, "Review": Review, "date": date, "Title": Title, "Author": Author}
            #if is_in_daterange == True:
            #    review_details.append(review_details_raw)
            #else:
            #    pass
        return review_details

    def combine_details(self,product_id):
        sku_brand_details = self.get_sku_data(product_id)
        for x in sku_brand_details:
            SKU, Brand, Root = x['SKU'], x['Brand'], x['Root']
        Product_Rating_details = self.get_product_rating(product_id)
        for y in Product_Rating_details:
            RatingCount, Product_Rating = y['RatingCount'], y['Product_Rating']
        Review_details = self.generate_reviews(product_id)
        print(SKU)
        for u in Review_details:
            u['SKU'], u['Brand'], u['Root'], u['RatingCount'], u['Product_Rating'] = SKU, Brand, Root, RatingCount, Product_Rating
        return Review_details

    def _to_csv(self, product_id):
        details = self.combine_details(product_id)
        dff = pd.DataFrame.from_dict(details)
        dff = dff.drop_duplicates()
        dff.to_csv("Riteaid_Reviews" + str(product_id) + ".csv", index=False)
        return dff

    def _read_csv(self, input_file_path):
        row_list = []
        with open(input_file_path, 'r', newline="") as bf:
            csvf = csv.DictReader(bf, delimiter=',', quotechar='"')
            field_names = csvf.fieldnames
            for row in csvf:
                row_list.append(row["link"])
            return row_list

    def _riteaid(self, row_list):
        row_list = self._read_csv(self.input_file_path)
        for row in row_list:
            self._validate_link(row)

    def _validate_link(self, link):
        split = link.split('/')[3]
        if split == "shop":
            product_id = self._get_url_data(link)
            # print(product_id)
        else:
            print("Invalid link")

    def _get_product_id(self, dom):
        product_id = dom.xpath('/html[1]/body[1]/div[3]/main[1]/div[2]/div[1]/div[3]/div[1]/div[4]/div[1]/div[1]/p[1]')[
            0].text
        product_id = product_id.replace('Item No. ', '')
        return product_id

    def _get_url_data(self, root):
        print(root)
        headers, payload = self._get_parameters()
        url_data = requests.request("GET", root, headers=headers, data=payload)
        soup = BeautifulSoup(url_data.content, "html.parser")
        dom = etree.HTML(str(soup))
        product_id = self._get_product_id(dom)
        dff = self._to_csv(product_id)
        return dff


def main(input_file_path):
    Result = RiteaidReviewScraper(input_file_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file_path", help="input_file_path")
    args = parser.parse_args()
    main(args.input_file_path)
