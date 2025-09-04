from sibr_api import ApiBase,NotFoundError
import os
import json
from aiohttp import BasicAuth
from sibr_module import BigQuery,Logger
import pandas as pd
from typing import Literal
import aiohttp
import asyncio
import inspect
import requests
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
#os.chdir("..")
#from dotenv import load_dotenv
#load_dotenv()

class EninApi(ApiBase):
    def __init__(self, logger=None, logger_name=None):
        super().__init__(logger, logger_name)
        if logger is None:
            logger = Logger("EninApi")
        self.logger = logger
        # self.logger.log_level = "DEBUG"
        self.base_url = "https://api.enin.ai/datasets/v1"
        self.auth = self.load_auth()
        self.bq = BigQuery(logger=logger)

    def load_auth(self):
        with open(os.getenv("ENIN_CREDENTIALS_PATH"), "r") as f:
            auth_data = json.load(f)
            return BasicAuth(auth_data['client_id'], auth_data['client_secret'])

    async def handle_jsonl_stream(self, response):
        results = []
        # BRUK "async for" og "response.content" for å strømme asynkront
        async for line in response.content:
            if line:
                try:
                    record = json.loads(line.decode('utf-8'))
                    results.append(record)
                except json.JSONDecodeError as e:
                    print(f"Kunne ikke parse linje: {line.decode('utf-8')}, Feil: {e}")
        return results

    async def get_companie_page(self, offset=0):
        PAGE_SIZE = 500
        url_company_page = f"{self.base_url}/dataset/company"

        params_company_page = {
            "response_file_type": "jsonl",
            "company.org_nr_schema": "NO",
            "limit": PAGE_SIZE,
            "offset": offset,
            "order_by_fields": "company.insert_timestamp"
        }

        response = await self.fetch_single(url=url_company_page,
                                           params=params_company_page,
                                           auth=self.auth,
                                           response_handler=self.handle_jsonl_stream,
                                           )

        return response if response else []

    async def get_companies(self, n: int):
        results = []
        for i in range(0, n, 500):
            self.logger.info(i)
            results += await self.get_companies(i)
        return pd.DataFrame.from_dict(results)

    async def get_item(self, item):
        url = f'https://api.enin.ai/analysis/v1/company/NO{item}/accounts-composite?accounts_type_identifier=annual_company_accounts'
        try:
            response = await self.fetch_single(url=url,
                                               auth=self.auth,
                                               timeout=90)
            return response if response else []
        except NotFoundError:
            self.logger.error(f'Item {item} not found. Returning None. 404 error')
            return None


    def save_func(self, data: dict[str, pd.DataFrame], if_exists: str = "merge"):

        def prepare_dataframe(df):
            string_cols = ["company_uuid", "accounts_type_uuid", "app_url", "uuid", "accounts_uuid", "currency_code",
                           "accounting_schema", "income_statement__currency_code"]
            bool_cols = ["corporate_group_accounts_flag", "estimated_accounting_period_flag"]
            date_cols = ["accounting_announcement_date", "accounting_from_date", "accounting_to_date"]

            # === HANDLE NUMERIC COLUMNS ===
            num_cols = ['inventory_change']
            for col in df.columns:
                if col not in string_cols and col not in bool_cols and col not in date_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    df[col] = df[col].fillna(0)
                    # df[col] = df[col].astype("float")
                elif col in date_cols:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            return df

        for key, df in data.items():
            self.logger.info(f'Processing {key} with {len(df)} records')
            if not df.empty or df is None:
                df.dropna(subset=["org_nr"], inplace=True)
                self.logger.info(f'{len(df)} records after dropping NaNs on org_nr')

                # == HANDLE TROBULE COLUMNS ===
                trouble_columns = []
                for col in trouble_columns:
                    if col in df.columns:
                        df[col] = df[col].astype(str)

                if key in ["company", 'accounts_type']:
                    df.drop_duplicates(subset=["org_nr"],inplace=True)
                    if if_exists == "merge":
                        self.bq.to_bq(df=df,
                                      table_name=key,
                                      dataset_name="enin",
                                      if_exists="merge",
                                      merge_on=["org_nr"],
                                      )
                    else:
                        self.bq.to_bq(df=df,
                                      table_name=key,
                                      dataset_name="enin",
                                      if_exists="replace")
                else:
                    df = prepare_dataframe(df)
                    df.drop_duplicates(subset= ["org_nr", "accounting_year"],inplace=True)
                    self.logger.info(f'{len(df)} records after preparing {key}')

                    if if_exists == "merge":
                        self.bq.to_bq(df=df,
                                      table_name=key,
                                      dataset_name="enin",
                                      if_exists="merge",
                                      merge_on=["org_nr", "accounting_year"],
                                      # explicit_schema = {"inventory_change" : "FLOAT"}
                                      )
                    else:
                        self.bq.to_bq(df=df,
                                      table_name=key,
                                      dataset_name="enin",
                                      if_exists="replace")

    def transform_data(self, items) -> dict:
        data = {
            'accounts': [],
            'accounts_highlights': [],
            'accounts_income_statement': [],
            'accounts_balance_sheet': [],
            'company': [],
            'accounts_type': [],
        }
        results = [self.transform_single(item) for item in items if item is not None]
        for i, result in enumerate(results):
            self.logger.debug(f'Working result nr {i}')
            for key in result:
                self.logger.debug(f'Extending data {key}...')
                data.get(key).extend(result[key])

        output = {}
        for key, value in data.items():
            df = pd.DataFrame(value)
            output[key] = df
        return output

    def transform_single(self, output):
        orgnr, res = output

        data = {
            'accounts': [],
            'accounts_highlights': [],
            'accounts_income_statement': [],
            'accounts_balance_sheet': [],
            'company': [],
            'accounts_type': [],
        }

        if res and len(res) > 0:
            self.ok_responses += 1
            for year in res:
                for key in data:
                    if key not in ["company", 'accounts_type']:
                        self.logger.debug(f'Adding data {key}')
                        sub_data = year.get(key)
                        sub_data["org_nr"] = int(orgnr)
                        data.get(key).append(sub_data)

            for key in ["company", 'accounts_type']:
                self.logger.debug(f'Adding data {key}')
                sub_data = res[0].get(key)
                sub_data["org_nr"] = int(orgnr)
                data.get(key).append(sub_data)
            return data
        else:
            self.fail_responses += 1
            return data

class RateLimitException(Exception):
    """Custom exception for API rate limiting errors (HTTP 429)."""
    pass

class BRREGapi(ApiBase):

    def __init__(self, logger : Logger = None,logger_name = None):
        super().__init__(logger,logger_name)
        self._base_url = "https://data.brreg.no/enhetsregisteret/api/enheter"
        if not logger:
            logger = Logger('brregAPI','a')
        self.logger = logger
        self.exceeds_limit = {"postnummer":{},
                              "kommunenummer" : {},
                              "nace" : {},
                              "nace_kommunenummer" : {},
                              "nace_date" : {},
                              "nace_municipality" : {},
                              "nace_postal" : {},
                              }
        self.bq = BigQuery(logger = logger)
        self.logger.set_level('INFO')


    async def get_company(self,orgnr : int | str):
        url = f"https://data.brreg.no/enhetsregisteret/api/enheter/{str(orgnr)}"
        headers = {
            'accept': 'application/json'
        }
        try:
            response = await self.fetch_single(url = url,
                                               headers = headers,
                                               return_format='json')
            return response

        except NotFoundError:
            self.logger.error(f'Item {orgnr} not found. Returning None')
            return None


    async def get_financial(self,orgnr : int | str):
        url = f'https://data.brreg.no/regnskapsregisteret/regnskap/{str(orgnr)}'

        headers = {
            'accept': 'application/json'
        }
        try:
            response = await self.fetch_single(url=url,
                                               headers=headers,
                                               return_format='json')
            if response:
                self.ok_responses += 1
                return response
            else:
                self.fail_responses += 1
                self.logger.error(f'API returner None.')
                return None
        except NotFoundError:
            self.fail_responses += 1
            if self.fail_responses % 100 == 0:
                self.logger.error(f'Item {orgnr} not found. Returning None. 404 Error')
            return None

    async def get_role(self,orgnr : int | str,timeout = 60):
        url = f'https://data.brreg.no/enhetsregisteret/api/enheter/{orgnr}/roller'

        headers = {
            'accept': 'application/json'
        }
        try:
            response = await self.fetch_single(url=url,
                                               headers=headers,
                                               return_format='json',
                                               timeout=timeout)
            return response
        except NotFoundError:
            self.fail_responses += 1
            if self.fail_responses % 100 == 0:
                self.logger.error(f'Item {orgnr} not found. Returning None')
            return None

    async def get_nace(self, nace_code):

        url = "https://data.brreg.no/enhetsregisteret/api/enheter"
        params = {"page": 0,
                  "size": 10000,
                  "naeringskode" : nace_code,
                  }
        headers = {
            'accept': 'application/json'
        }
        return await self.get_page(arg=nace_code,
                                   arg_type="nace",
                                   url=url,
                                   params=params,
                                   headers=headers)

    async def get_page_by_municipality(self,municipality_code):

        url = "https://data.brreg.no/enhetsregisteret/api/enheter"
        params = {"page": 0,
                  "size": 10000,
                  "forretningsadresse.kommunenummer" : municipality_code,
                  }
        headers = {
            'accept': 'application/json'
        }
        return await self.get_page(arg=municipality_code,
                                   arg_type="kommunenummer",
                                   url=url,
                                   params=params,
                                   headers=headers)

    async def get_page_by_postal_code(self,postal_code):

        url = "https://data.brreg.no/enhetsregisteret/api/enheter"

        params = {"page": 0,
                  "size": 10000,
                  "forretningsadresse.postnummer" :postal_code,
                  }
        headers = {
            'accept': 'application/json'
        }
        return await self.get_page(arg = postal_code,
                                   arg_type = "postnummer",
                                   url = url,
                                   params = params,
                                   headers = headers)

    async def get_nace_date(self,args):
        nace_code,from_to = args
        url = "https://data.brreg.no/enhetsregisteret/api/enheter"

        params = {"page": 0,
                  "size": 10000,
                  "naeringskode" : nace_code,
                  f"{from_to}Stiftelsesdato": "2007-01-01",
                  }
        headers = {
            'accept': 'application/json'
        }
        return await self.get_page(arg=args,
                                   arg_type="nace_date",
                                   url=url,
                                   params=params,
                                   headers=headers)

    async def get_nace_municipality(self,args):
        nace_code,municipality_code = args
        url = "https://data.brreg.no/enhetsregisteret/api/enheter"

        params = {"page": 0,
                  "size": 10000,
                  "naeringskode" : nace_code,
                  "forretningsadresse.kommunenummer" : municipality_code
                  }
        headers = {
            'accept': 'application/json'
        }
        return await self.get_page(arg=args,
                                   arg_type="nace_municipality",
                                   url=url,
                                   params=params,
                                   headers=headers)

    async def get_nace_postal(self,args):
        nace_code,postal_code = args
        url = "https://data.brreg.no/enhetsregisteret/api/enheter"

        params = {"page": 0,
                  "size": 10000,
                  "naeringskode" : nace_code,
                  "forretningsadresse.postnummer" : postal_code
                  }
        headers = {
            'accept': 'application/json'
        }
        return await self.get_page(arg=args,
                                   arg_type="nace_postal",
                                   url=url,
                                   params=params,
                                   headers=headers)

    async def get_page(self,arg,arg_type,url,params,headers):
        try:
            response = await self.fetch_single(url=url, params=params, headers=headers, timeout=30)
            if response:
                total_elements = response.get("page").get("totalElements")
                self.logger.debug(f'Getting data from {arg_type} {arg} with total elements: {total_elements}')
                if total_elements > 10000:
                    self.logger.error(
                        f'Total elements is {total_elements} for {arg_type} {arg}, which exceeds the limit.\nAdding it to {self.exceeds_limit.get(arg_type)}')
                    if isinstance(arg,tuple) or isinstance(arg,list):
                        arg = str(arg)
                    self.exceeds_limit.get(arg_type)[arg] = total_elements
                    return None
                elif total_elements == 0:
                    self.logger.warning(f'Zero elements found for {arg_type} {arg} in api. Returning None')
                    self.fail_responses += 1
                    return None
                else:
                    self.ok_responses += 1
                    return response
            else:
                self.logger.warning(f'Api returned None for {arg_type} {arg}')

        except NotFoundError:
            self.fail_responses += 1
            if self.fail_responses % 100 == 0:
                self.logger.warning(f'No data found for {arg_type} {arg} - 404 error')
            return None

    def transform_pages(self,items):

        def transform_single(item):
            if item:
                data = item.get("_embedded",{}).get("enheter")
                if data:
                    #self.ok_responses += 1
                    df = pd.json_normalize(data)
                    self._ensure_fieldnames(df)
                    df["fetch_date"] = pd.Timestamp.now()
                    return df
                else:
                    #self.fail_responses += 1
                    self.logger.warning(f'No data found when transforming {item}')
                    return None
            else:
                #self.fail_responses += 1
                self.logger.warning(f'No data found: {item}')
                return None
        results = [transform_single(item) for item in items if item is not None]
        return pd.concat(results, ignore_index=True)

    def transform_roles(self,items):

        def transform_roles_single(item) -> pd.DataFrame | None:
            orgnr, raw = item
            df = pd.DataFrame()
            if raw:
                role_groups = raw.get('rollegrupper', [])
                if role_groups:
                    #self.ok_responses += 1
                    for group in role_groups:
                        df1 = pd.json_normalize(group['roller'])
                        df1['organisasjonsnummer'] = orgnr
                        df = pd.concat([df, df1], ignore_index=True)
                    self._ensure_fieldnames(df)
                    df["fetch_date"] = pd.Timestamp.now()
                    return df
                else:
                    #self.fail_responses += 1
                    self.logger.warning(f'No role groups found for {orgnr}')
                    return None
            else:
                #self.fail_responses += 1
                self.logger.warning(f'No results from fetcher for {orgnr}')
                return None

        results = [transform_roles_single(item) for item in items if item is not None]
        return pd.concat(results, ignore_index=True)

    def save_pages(self,df,if_exists : Literal["merge","append","replace"] = "merge"):

        def deep_merge(dict1, dict2):
            """Slår sammen dict2 inn i dict1 på en 'dyp' måte."""
            for key, value in dict2.items():
                if key in dict1 and isinstance(dict1[key], dict) and isinstance(value, dict):
                    # Hvis nøkkelen finnes i begge og begge verdier er ordbøker,
                    # kall funksjonen på nytt for de indre ordbøkene.
                    deep_merge(dict1[key], value)
                else:
                    # Ellers, bare oppdater/sett verdien.
                    dict1[key] = value
            return dict1

        exceeds_limit_file = "exceeds_limit.json"

        # Bare utfør filoperasjoner hvis det faktisk er nye data å lagre
        if self.exceeds_limit:
            existing_data = {}
            # 1. Prøv å lese eksisterende data fra filen
            try:
                with open(exceeds_limit_file, "r") as f:
                    existing_data = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                pass

            data = deep_merge(existing_data, self.exceeds_limit)

            # 3. Skriv den fullstendige, oppdaterte dict-en tilbake til filen
            with open(exceeds_limit_file, "w") as f:
                # indent=4 gjør filen mer lesbar for mennesker
                json.dump(data, f, indent=4)

                # 4. Tøm listen for neste kjøring
            #self.exceeds_limit.clear()

        table = "companies"
        dataset = "brreg"
        merge_on = ["organisasjonsnummer"]
        #self._ensure_fieldnames(df)
        df.dropna(subset=merge_on, inplace=True)
        trouble_cols = ["paategninger"]
        for col in trouble_cols:
            df[col] = df[col].astype(str)

        not_found = ["underUtenlandskInsolvensbehandlingDato"]
        for col in not_found:
            if col not in df.columns:
                df[col] = None
        explicit_columns = {"aktivitet": ("STRING", "REPEATED"),
                            #"paategninger": ("STRING", "REPEATED"),
                            "postadresse_adresse": ("STRING", "REPEATED"),
                            "forretningsadresse_adresse": ("STRING", "REPEATED"),
                            "vedtektsfestetFormaal": ("STRING", "REPEATED"),
                            "fetch_date": "DATETIME"
                            }
        if if_exists=="merge":
            df.drop_duplicates(subset=merge_on, inplace=True)
            self.logger.info(f'Drop duplicates before merge. Len after drop duplicates {len(df)}')
            self.bq.to_bq(df, table, dataset, if_exists='merge', merge_on=merge_on,explicit_schema=explicit_columns)
        # else:
        #     self.bq.to_bq(df, table, dataset, if_exists=if_exists,explicit_schema=explicit_columns)

    def save_roles(self,df,if_exists : Literal["merge","append","replace"] = "append"):
        table = "roles"
        dataset = "brreg"
        merge_on = ["organisasjonsnummer"]
        df.dropna(subset=merge_on, inplace=True)
        if if_exists == "merge":
            df.drop_duplicates(subset=merge_on, inplace=True)
            self.logger.info(f'Drop duplicates before merge. Len after drop duplicates {len(df)}')
            self.bq.to_bq(df, table, dataset, if_exists='merge', merge_on=merge_on,explicit_schema={"fetch_date" : "DATETIME"})
        # else:
        #     self.bq.to_bq(df, table, dataset, if_exists=if_exists)

    def transformer(self,items):
        def transform_single(item):
            org_nr,data = item
            if data:
                #self.ok_responses += 1
                df = pd.json_normalize(data)
            else:
                #self.fail_responses += 1
                df = pd.DataFrame()
            df["organisasjonsnummer"] = org_nr
            return df

        results = [transform_single(item) for item in items if item is not None]
        df = pd.concat(results, ignore_index=True)
        self._ensure_fieldnames(df)
        if not df.empty and df is not None:
            return df
        else:
            self.logger.warning(f'No data found in transform! Items {items[:5]}...')


    def saver(self,df,if_exists : Literal["merge","append","replace"] = "merge"):

        merge_on = ["virksomhet_organisasjonsnummer","regnskapsperiode_fraDato","regnskapsperiode_tilDato"]
        df.drop_duplicates(subset = merge_on, inplace=True)

        if if_exists == "merge":
            self.bq.to_bq(df = df,
                          table_name = "financial",
                          dataset_name = "brreg",
                          if_exists = if_exists,
                          merge_on = merge_on)
        # else:
        #     self.bq.to_bq(df=df,
        #                   table_name="financial",
        #                   dataset_name="brreg",
        #                   if_exists=if_exists)


    # async def get_by_nace_geo(self,
    #                           nace_codes : list = None,
    #                           geo_type: Literal["kommune","kommunenummer","postnummer","poststed"] = "kommunenummer",
    #                           geo_value : list = None,
    #                           batch_size = 200,
    #                           save_interval = 50000,
    #                           save_bq=False) -> pd.DataFrame | None:
    #     '''
    #     Fetches company data based on NACE code or geographical location.
    #     If either one of the parameters is None, it will not be used in the query.
    #     The data is transformed to a Pandas DataFrame and exported to Big Query Table `brreg.company_data`
    #     :param nace_codes:
    #     :param geo_type:
    #     :param geo_value:
    #     :param save_bq:
    #     :return:
    #     '''
    #
    #     self.logger.info("\n \n Starting new session with NACE/GEO funksjon")
    #     if geo_type not in ["kommune","kommunenummer","postnummer","poststed"]:
    #         raise ValueError(f"Invalid geo_type: {geo_type}. Choose between 'kommune', 'kommunenummer', 'postnummer' and 'poststed'.")
    #
    #     if geo_value is None and nace_codes is None:
    #         raise ValueError("Either geo_value or nace_codes must be provided.")
    #
    #     if nace_codes is not None and not isinstance(nace_codes, list):
    #         nace_codes = [nace_codes]
    #     if geo_value is not None and not isinstance(geo_value, list):
    #         geo_value = [geo_value]
    #
    #     antall_per_side = 100
    #     BATCH_SIZE = batch_size
    #     SAVE_INTERVAL = save_interval
    #     total_companies = 0
    #     total_count = 0
    #     dataframes = []
    #
    #     async def fetch_single(nace=None,geo=None)->pd.DataFrame | None:
    #         #session = await self._reset_session()
    #         companies = []
    #         side = 0
    #
    #         while True:
    #             params = {
    #                 "page": side,
    #                 "size": antall_per_side,
    #                 "naeringskode": nace,
    #                 f"forretningsadresse.{geo_type}": geo
    #             }
    #             params = {k: v for k, v in params.items() if v is not None}  # Fjern None-verdier
    #
    #             #print(params,self._base_url)
    #             response =  await self._request(self._base_url, params)
    #             if response is None:
    #                 return None
    #
    #             if response.status == 200:
    #                 # self.logger.debug(f'Got response 200 for {nace},{geo}')
    #                 data = await response.json()
    #                 selskaper = data.get("_embedded", {}).get("enheter", [])  # liste av selskaper
    #                 if not selskaper:
    #                     break
    #                 df = pd.json_normalize(selskaper)
    #                 df['page'] = side
    #                 side += 1
    #                 companies.append(df)
    #                 self.logger.debug(f'Found {len(df)} for {nace} and {geo} on page {side} and added to companies')
    #             elif response.status == 429:
    #                 error_text = await response.text()
    #                 self.logger.error(f"error code 429 {error_text}")
    #                 raise RateLimitException(f"Rate limit exceeded (429) for NACE: {nace}, Geo: {geo}")
    #             else:
    #                 error_text = await response.text()
    #                 self.logger.error(
    #                     f'Error message - {inspect.currentframe().f_code.co_name}: nace code {nace}, geo {geo}, {response.status}, {error_text}')
    #                 break
    #
    #             # try:
    #             #     async with session.get(self._base_url, params=params) as response:
    #             # except Exception as e:
    #             #     tb = traceback.format_exc()  # Hent full stack trace
    #             #     self.logger.error(f"Stack trace: {tb}")  # Logg stack trace
    #             #     self.logger.error(
    #             #         f"Error processing nace {nace} and geo {geo} with geotype {geo_type}: {e} | Function: {inspect.currentframe().f_code.co_name}")
    #             #     break
    #
    #         if companies:
    #             return pd.concat(companies, ignore_index=True)
    #         return None
    #     async def process_batch(tasks) -> list[pd.DataFrame]:
    #         self.logger.debug('Doing gathering of tasks')
    #         results = await asyncio.gather(*tasks,return_exceptions=True)
    #         valid = []
    #
    #         for res in results:
    #             if isinstance(res,RateLimitException):
    #                 raise res
    #             if isinstance(res,Exception):
    #                 self.logger.error(f'Task error: {res}')
    #             if isinstance(res,pd.DataFrame) and not res.empty:
    #                 valid.append(res)
    #         self.logger.debug(f'Gathered {len(valid)} dataframes')
    #         return valid
    #     def prep_save(frames):
    #         table = 'company_data'
    #         dataset = 'brreg'
    #         if not frames:
    #             self.logger.info(f'No data frames to save with nace {nace_codes[:10]} and {geo_type} {geo_value}')
    #             return []
    #         df = pd.concat(frames, ignore_index=True)
    #         df = df.drop_duplicates(subset = ["organisasjonsnummer"])
    #         self._ensure_fieldnames(df)
    #         trouble_columns = [
    #                         "paategninger",
    #                         #"vedtektsfestetFormaal",
    #                         #"aktivitet",
    #                         #"forretningsadresse_adresse",
    #                         ]
    #         for col in trouble_columns:
    #             if col in df.columns:
    #                 df[col] = df[col].apply(lambda x: str(x))
    #         df['country'] = 'NO'
    #         df['page'] = df['page'].astype(int)
    #         df["fetch_date"] = pd.Timestamp.now()
    #         # orgnrliste = df['organisasjonsnummer'].astype(str).to_list()
    #         # if len(orgnrliste) == 1:
    #         #     query = f''' SELECT organisasjonsnummer FROM {dataset}.{table}
    #         #                 WHERE organisasjonsnummer = '{orgnrliste[0]}'
    #         #                 '''
    #         # else:
    #         #     query = f''' SELECT organisasjonsnummer FROM {dataset}.{table}
    #         #                                 WHERE organisasjonsnummer IN {tuple(orgnrliste)}
    #         #                                 '''
    #         # # self.logger.info(query)
    #         # # self.logger.info(f'Fetched {len(df)} companies from nace {nace_codes} and {geo_value}')
    #         # overlaping = self.bq.read_bq(query)
    #         # df_new = df[~df['organisasjonsnummer'].isin(overlaping)]
    #         # if not df_new.empty:
    #         #     self.logger.info(
    #         #         f"Got {len(df)} companies from API call. Removing {len(overlaping)} that already exists in DB, adding {len(df_new)} new ones.")
    #         self.bq.to_bq(df = df,
    #                        table_name=table,
    #                        dataset_name = dataset,
    #                         if_exists='merge',
    #                        merge_on = ['organisasjonsnummer']
    #                         )
    #
    #     starttime = datetime.now()
    #     self.logger.info(f"Fetching data for NACE codes: {nace_codes[:10]} and geo_type: {geo_type} with values: {geo_value}")
    #
    #     combinations = []
    #     if nace_codes and geo_value:
    #         for nace in nace_codes:
    #             for geo in geo_value:
    #                 combinations.append((nace,geo))
    #     elif nace_codes:
    #         for nace in nace_codes:
    #             combinations.append((nace,None))
    #     else:
    #         for geo in geo_value:
    #             combinations.append((None,geo))
    #
    #     for i in range(0,len(combinations),BATCH_SIZE):
    #
    #         comb = combinations[i:i+BATCH_SIZE]
    #         tasks = [fetch_single(nace,geo) for nace,geo in comb]
    #         self.logger.debug(f'fetching combination: {comb}')
    #
    #         batch_df = await process_batch(tasks)
    #         dataframes.extend(batch_df)
    #
    #         total_companies += sum(len(df) for df in batch_df)
    #         self.logger.debug(f'Processed {total_companies} companies so far')
    #
    #         if save_bq and total_companies>=SAVE_INTERVAL:
    #             if dataframes:
    #                 prep_save(dataframes)
    #                 dataframes = []
    #                 total_count += total_companies
    #                 self.logger.info(f"Fetched {total_count} so far...")
    #                 total_companies = 0
    #
    #     if save_bq:
    #         if dataframes:
    #             prep_save(dataframes)
    #             total_count += total_companies
    #
    #     self.logger.info(f'Task completed in {datetime.now() - starttime}, got {total_count} companies')
    #
    #
    # async def fill_roles(self):
    #     query = '''SELECT DISTINCT(c.organisasjonsnummer)
    #             FROM `brreg.company_data` c
    #             LEFT JOIN `brreg.roles` r ON c.organisasjonsnummer = r.organisasjonsnummer
    #             WHERE r.organisasjonsnummer IS NULL
    #             '''
    #     orgs = self.bq.read_bq(query)
    #     orgs = orgs['organisasjonsnummer'].tolist()
    #     self.logger.info(f"Found {len(orgs)} companies without role data")
    #     if orgs:
    #         return await self.get_roles(orgs,save_bq=True)
    #
    # async def fill_financials(self,head=None, year : int = 2024):
    #     query = f'''SELECT c.organisasjonsnummer
    #     FROM `brreg.company_data` c
    #     WHERE NOT EXISTS (
    #         SELECT 1 FROM `brreg.financial` f
    #         WHERE f.virksomhet_organisasjonsnummer = c.organisasjonsnummer
    #         AND f.regnskapsperiode_fraDato = "{year}-01-01"
    #     )
    #     '''
    #     if head:
    #         query += f'LIMIT {head}'
    #     orgs = self.bq.read_bq(query)
    #     orgs = orgs['organisasjonsnummer'].tolist()
    #     self.logger.info(f"Found {len(orgs)} companies without financial data")
    #     if orgs:
    #         return await self.get_financial_data(org_nums=orgs,
    #                                              save_bq=True)
    #
    # async def fill_companies(self,org_nums):
    #     await self.get_financial_data(org_nums,save_bq=True)
    #     await self.get_companies(org_nums,save_bq=True)
    #     await self.get_roles(org_nums,save_bq=True)

