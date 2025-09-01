from sibr_api import ApiBase
import os
import json
from aiohttp import BasicAuth
from sibr_module import BigQuery,Logger
import pandas as pd
from typing import Literal
import aiohttp
import asyncio
import inspect
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
        response = await self.fetch_single(url=url,
                                           auth=self.auth, )
        return response if response else []

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

                if if_exists == "merge":
                    if key in ["company", 'accounts_type']:
                        self.bq.to_bq(df=df,
                                      table_name=key,
                                      dataset_name="enin",
                                      if_exists="merge",
                                      merge_on=["org_nr"],
                                      )
                    else:
                        df = prepare_dataframe(df)
                        self.logger.info(f'{len(df)} records after preparing {key}')
                        self.bq.to_bq(df=df,
                                      table_name=key,
                                      dataset_name="enin",
                                      if_exists="merge",
                                      merge_on=["org_nr", "accounting_year"],
                                      # explicit_schema = {"inventory_change" : "FLOAT"}
                                      )
                if if_exists == "replace":

                    if key in ["company", 'accounts_type']:
                        self.bq.to_bq(df=df,
                                      table_name=key,
                                      dataset_name="enin",
                                      if_exists="replace")
                    else:
                        df = prepare_dataframe(df)
                        self.logger.info(f'{len(df)} records after preparing {key}')
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

class BRREGapi:

    def __init__(self, logger : Logger = None):
        self._base_url = "https://data.brreg.no/enhetsregisteret/api/enheter"
        if not logger:
            logger = Logger('brregAPI','a')
        self.logger = logger
        self._headers = {
            'accept': 'application/json'
        }
        self._session = None
        self._bq = BigQuery(logger = logger)
        self.logger.set_level('INFO')


    def _ensure_fieldnames(self,df):
        new_cols = []
        for col in df.columns:
            new_cols.append(col.replace('.', '_',))
        df.columns = new_cols
        if list(df.columns).count('virksomhet_organisasjonsnummer')>1:
            self.logger.error(f'Duplicate columns found: {df.columns}')
            raise ValueError(f"Duplicate columns found: {df.columns}")
    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=self._headers)
        return self._session
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
    async def _reset_session(self):
        if self._session:
            await self._session.close()
        self._session = None
        return await self._ensure_session()

    async def _request(self, url: str, params: dict = None) -> aiohttp.ClientResponse | None:
        """
        Utfører et nettverkskall og returnerer hele respons-objektet.
        Håndterer kun generelle tilkoblingsfeil.
        """
        try:
            session = await self._ensure_session()
            return await session.get(url, headers=self._headers, params=params)
        except Exception as e:
            self.logger.error(f"Tilkoblingsfeil for URL {url}: {e}")
            return None

    def save_bq(self,df,key_col,table,dataset):
        df = df.dropna(subset=[key_col])
        if df.empty:
            self.logger.warning("DataFrame er tom etter fjerning av NaN-verdier")
            return df
        self.logger.debug(f"DataFrame før BigQuery-lagring:\n{df.head()}")
        self.logger.debug(f"Kolonner: {df.columns.tolist()}")
        self.logger.debug(f"Datatyper:\n{df.dtypes}")
        self._bq.to_bq(df, table, dataset, if_exists='append')

    async def get_companies(self, org_nums: list,save_bq = False) -> pd.DataFrame|None:
        data_frames = []
        SAVE_INTERVAL = 5000
        BATCH_SIZE = 200
        base_url = "https://data.brreg.no/enhetsregisteret/api/enheter/"

        async def fetch_single(orgnr):
            response = await self._request(base_url + str(orgnr))
            if not response:
                return None

            if response.status == 200:
                data = await response.json()
                return pd.json_normalize(data)
            else:
                error_text = await response.text()
                self.logger.error(
                    f'Error message - {inspect.currentframe().f_code.co_name}: orgnr {orgnr}, {response.status}, {error_text}')

        for i in range(0, len(org_nums), BATCH_SIZE):
            batch_orgs = org_nums[i:i + BATCH_SIZE]
            tasks = [fetch_single(orgnr) for orgnr in batch_orgs]
            self.logger.debug(f'Fetching batch: {batch_orgs[:10]}..Truncated')

            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            batch_results = [result for result in batch_results if result is not None and ((hasattr(result, 'empty') and not result.empty))]
            data_frames.extend(batch_results)

            if len(data_frames) >= SAVE_INTERVAL:
                df = pd.concat(data_frames, ignore_index=True)
                self._ensure_fieldnames(df)
                df['country'] = 'NO'
                df["fetch_date"] = pd.Timestamp.now()
                if save_bq:
                    self.save_bq(df,'organisasjonsnummer',table='companies',dataset='brreg')
                data_frames = []

            if save_bq and data_frames:
                if len(data_frames)>1:
                    df = pd.concat(data_frames, ignore_index=True)
                elif len(data_frames)==1:
                    df = data_frames[0]
                else:
                    raise ValueError("no data frames")
                self._ensure_fieldnames(df)
                df['country'] = 'NO'
                df["fetch_date"] = pd.Timestamp.now()
                if 'paategninger' in df.columns:
                    df['paategninger'] = df['paategninger'].astype(str)
                self.logger.info(f"Processed {len(data_frames)} organizations so far")
                self.save_bq(df, 'organisasjonsnummer', table='company_data', dataset='brreg')

    async def get_financial_data(self, org_nums: list,save_bq=False) -> list[pd.DataFrame] | None:
        saved_frames = []
        data_frames = []

        SAVE_INTERVAL = 5000
        BATCH_SIZE = 200
        count = 0
        count_ok = 0
        count_fail = 0
        base_url = 'https://data.brreg.no/regnskapsregisteret/regnskap/'

        async def fetch_single(orgnr) -> pd.DataFrame | None:
            nonlocal count,count_ok,count_fail
            count += 1

            if count % 1000==0:
                self.logger.info(f"Processed {count} organizations. {count_ok} OK, {count_fail} failed")
        #
            self.logger.debug(f"Henter data fra: {base_url + str(orgnr)}")

            response = await self._request(base_url + str(orgnr))
            if response is None:
                return None

            if response.status == 200:
                count_ok += 1
                data = await response.json()
                if data and len(data) > 0:
                    return pd.json_normalize(data[0])
                else:
                    self.logger.info(f'No data found for orgnr {orgnr}')
                    self.logger.info(f'Data is empty: {data}')
                    return None
                    # return pd.DataFrame([{'virksomhet_organisasjonsnummer_empty': str(orgnr), }])
            elif response.status == 404 and orgnr:
                count_fail += 1
                self.logger.debug(
                    f'No data found for orgnr {orgnr} with status code {response.status}. Adding {orgnr} with no financial data')
                return pd.DataFrame([{'virksomhet_organisasjonsnummer_empty': str(orgnr), }])

            elif response.status == 500 and 'Regnskapet inneholder en oppstillingsplan som ikke er stottet' in response.text():
                count_fail += 1
                self.logger.debug(
                    f'No data found for orgnr {orgnr} with status code {response.status}. Adding {orgnr} with no financial data')
                return pd.DataFrame([{'virksomhet_organisasjonsnummer_empty': str(orgnr), }])
            else:
                error_text = await response.text()
                self.logger.error(
                    f'Could not get data for orgnr {orgnr}. Error: {response.status}, {error_text} | Function: {inspect.currentframe().f_code.co_name}')
                return None

            # try:
            #     session = await self._ensure_session()
            #     async with session.get(url, headers=self._headers) as response:
            #         reponse_text = await response.text()
            #
            #
            # except Exception as e:
            #     tb = traceback.format_exc()  # Hent full stack trace
            #     self.logger.error(f"Stack trace: {tb}")  # Logg stack trace
            #     self.logger.error(f"Error processing orgnr {orgnr}: {e} | Function: {inspect.currentframe().f_code.co_name}")
            #     return None

        def prep_save(frames):
            if not frames:
                self.logger.info('No data frames to save')
                return None

            df_to_save = pd.concat(data_frames, ignore_index=True)
            self._ensure_fieldnames(df_to_save)
            df_to_save["fetch_date"] = pd.Timestamp.now()
            if 'virksomhet_organisasjonsnummer_empty' in df_to_save.columns:
                df_to_save['virksomhet_organisasjonsnummer'] = df_to_save['virksomhet_organisasjonsnummer_empty'].fillna(
                    df_to_save['virksomhet_organisasjonsnummer'])
                df_to_save.drop('virksomhet_organisasjonsnummer_empty', axis=1, inplace=True)
            return df_to_save

        for i in range(0, len(org_nums), BATCH_SIZE):
            batch_orgs = org_nums[i:i + BATCH_SIZE]
            batch_results = await asyncio.gather(*[fetch_single(orgnr) for orgnr in batch_orgs],return_exceptions=True)

            batch_results = [result for result in batch_results if result is not None and ((hasattr(result, 'empty') and not result.empty))]
            data_frames.extend(batch_results)
            saved_frames.extend(batch_results)

            if save_bq and count % SAVE_INTERVAL == 0:
                data_frames = prep_save(data_frames)
                self.save_bq(data_frames, 'virksomhet_organisasjonsnummer', table='financial', dataset='brreg')


        if save_bq:
            df = prep_save(data_frames)
            self.save_bq(df, 'virksomhet_organisasjonsnummer', table='financial', dataset='brreg')
        return prep_save(saved_frames)

    async def get_roles(self, org_nums: list,save_bq = False) -> pd.DataFrame|None:
                dataframes = []
                session = await self._ensure_session()
                SAVE_INTERVAL = 2000
                BATCH_SIZE = 200
                count = 0
                save_count = 0

                async def fetch_single(orgnr):
                    nonlocal count,save_count
                    base = f'https://data.brreg.no/enhetsregisteret/api/enheter/{orgnr}/roller'
                    response = await self._request(url = base)
                    if response.status == 200:
                        df = pd.DataFrame()
                        res = await response.json()
                        role_groups = res.get('rollegrupper', [])
                        if role_groups:
                            for group in role_groups:
                                df1 = pd.json_normalize(group['roller'])
                                df1['organisasjonsnummer'] = orgnr
                                df = pd.concat([df, df1], ignore_index=True)
                            count += 1
                            save_count += 1
                            self.logger.debug(
                                f'Got {len(df)} roles for {orgnr}. Count {count} | save_count {save_count}')
                            return df
                        else:
                            self.logger.error(f'No role groups found for {orgnr}')
                    elif response.status == 404 and orgnr:
                        count += 1
                        save_count += 1
                        self.logger.debug(
                            f'No roles {orgnr}. Count {count} | save_count {save_count}. Saved to dataframe')
                        return pd.DataFrame([{'organisasjonsnummer': str(orgnr), }])
                    else:
                        error_text = await response.text()
                        self.logger.error(
                            f'Error message - {inspect.currentframe().f_code.co_name}: orgnr {orgnr}, {response.status}, {error_text} | Count {count} | save_count {save_count}')


                for i in range(0, len(org_nums), BATCH_SIZE):
                    batch = org_nums[i:i + BATCH_SIZE]
                    tasks = [fetch_single(orgnr) for orgnr in batch]

                    res = await asyncio.gather(*tasks, return_exceptions=True)

                    res = [result for result in res if result is not None and ((hasattr(result, 'empty') and not result.empty))]
                    dataframes.extend(res)
                    self.logger.debug(f'Fetched {len(res)}. Dataframes {len(dataframes)} Count: {count}')

                    if count % 1000 == 0:
                        self.logger.info(f"Processed {count} organizations | save_count {save_count} | dataframes: {len(dataframes)}")

                    if save_count >= SAVE_INTERVAL and save_bq:
                        if dataframes:
                            df = pd.concat(dataframes, ignore_index=True)
                            self._ensure_fieldnames(df)
                            df['country'] = 'NO'
                            if 'stadfestetFremtidsfullmakt' in df.columns:
                                df['stadfestetFremtidsfullmakt'] = df['stadfestetFremtidsfullmakt'].astype(str)
                            if 'begrensetRettsligHandleevne' in df.columns:
                                df['begrensetRettsligHandleevne'] = df['begrensetRettsligHandleevne'].astype(str)
                            self.save_bq(df,'organisasjonsnummer',table='roles',dataset='brreg')
                            dataframes = []
                            save_count = 0

                if save_bq and dataframes:
                    df = pd.concat(dataframes, ignore_index=True)
                    self._ensure_fieldnames(df)
                    df['country'] = 'NO'
                    df["fetch_date"] = pd.Timestamp.now()
                    self.save_bq(df,'organisasjonsnummer',table='roles',dataset='brreg')

    async def get_by_nace_geo(self,
                              nace_codes : list = None,
                              geo_type: Literal["kommune","kommunenummer","postnummer","poststed"] = "kommunenummer",
                              geo_value : list = None,
                              batch_size = 200,
                              save_interval = 50000,
                              save_bq=False) -> pd.DataFrame | None:
        '''
        Fetches company data based on NACE code or geographical location.
        If either one of the parameters is None, it will not be used in the query.
        The data is transformed to a Pandas DataFrame and exported to Big Query Table `brreg.company_data`
        :param nace_codes:
        :param geo_type:
        :param geo_value:
        :param save_bq:
        :return:
        '''

        self.logger.info("\n \n Starting new session with NACE/GEO funksjon")
        if geo_type not in ["kommune","kommunenummer","postnummer","poststed"]:
            raise ValueError(f"Invalid geo_type: {geo_type}. Choose between 'kommune', 'kommunenummer', 'postnummer' and 'poststed'.")

        if geo_value is None and nace_codes is None:
            raise ValueError("Either geo_value or nace_codes must be provided.")

        if nace_codes is not None and not isinstance(nace_codes, list):
            nace_codes = [nace_codes]
        if geo_value is not None and not isinstance(geo_value, list):
            geo_value = [geo_value]

        antall_per_side = 100
        BATCH_SIZE = batch_size
        SAVE_INTERVAL = save_interval
        total_companies = 0
        total_count = 0
        dataframes = []

        async def fetch_single(nace=None,geo=None)->pd.DataFrame | None:
            #session = await self._reset_session()
            companies = []
            side = 0

            while True:
                params = {
                    "page": side,
                    "size": antall_per_side,
                    "naeringskode": nace,
                    f"forretningsadresse.{geo_type}": geo
                }
                params = {k: v for k, v in params.items() if v is not None}  # Fjern None-verdier

                #print(params,self._base_url)
                response =  await self._request(self._base_url, params)
                if response is None:
                    return None

                if response.status == 200:
                    # self.logger.debug(f'Got response 200 for {nace},{geo}')
                    data = await response.json()
                    selskaper = data.get("_embedded", {}).get("enheter", [])  # liste av selskaper
                    if not selskaper:
                        break
                    df = pd.json_normalize(selskaper)
                    df['page'] = side
                    side += 1
                    companies.append(df)
                    self.logger.debug(f'Found {len(df)} for {nace} and {geo} on page {side} and added to companies')
                elif response.status == 429:
                    error_text = await response.text()
                    self.logger.error(f"error code 429 {error_text}")
                    raise RateLimitException(f"Rate limit exceeded (429) for NACE: {nace}, Geo: {geo}")
                else:
                    error_text = await response.text()
                    self.logger.error(
                        f'Error message - {inspect.currentframe().f_code.co_name}: nace code {nace}, geo {geo}, {response.status}, {error_text}')
                    break

                # try:
                #     async with session.get(self._base_url, params=params) as response:
                # except Exception as e:
                #     tb = traceback.format_exc()  # Hent full stack trace
                #     self.logger.error(f"Stack trace: {tb}")  # Logg stack trace
                #     self.logger.error(
                #         f"Error processing nace {nace} and geo {geo} with geotype {geo_type}: {e} | Function: {inspect.currentframe().f_code.co_name}")
                #     break

            if companies:
                return pd.concat(companies, ignore_index=True)
            return None
        async def process_batch(tasks) -> list[pd.DataFrame]:
            self.logger.debug('Doing gathering of tasks')
            results = await asyncio.gather(*tasks,return_exceptions=True)
            valid = []

            for res in results:
                if isinstance(res,RateLimitException):
                    raise res
                if isinstance(res,Exception):
                    self.logger.error(f'Task error: {res}')
                if isinstance(res,pd.DataFrame) and not res.empty:
                    valid.append(res)
            self.logger.debug(f'Gathered {len(valid)} dataframes')
            return valid
        def prep_save(frames):
            table = 'company_data'
            dataset = 'brreg'
            if not frames:
                self.logger.info(f'No data frames to save with nace {nace_codes[:10]} and {geo_type} {geo_value}')
                return []
            df = pd.concat(frames, ignore_index=True)
            df = df.drop_duplicates(subset = ["organisasjonsnummer"])
            self._ensure_fieldnames(df)
            trouble_columns = [
                            "paategninger",
                            #"vedtektsfestetFormaal",
                            #"aktivitet",
                            #"forretningsadresse_adresse",
                            ]
            for col in trouble_columns:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: str(x))
            df['country'] = 'NO'
            df['page'] = df['page'].astype(int)
            df["fetch_date"] = pd.Timestamp.now()
            # orgnrliste = df['organisasjonsnummer'].astype(str).to_list()
            # if len(orgnrliste) == 1:
            #     query = f''' SELECT organisasjonsnummer FROM {dataset}.{table}
            #                 WHERE organisasjonsnummer = '{orgnrliste[0]}'
            #                 '''
            # else:
            #     query = f''' SELECT organisasjonsnummer FROM {dataset}.{table}
            #                                 WHERE organisasjonsnummer IN {tuple(orgnrliste)}
            #                                 '''
            # # self.logger.info(query)
            # # self.logger.info(f'Fetched {len(df)} companies from nace {nace_codes} and {geo_value}')
            # overlaping = self._bq.read_bq(query)
            # df_new = df[~df['organisasjonsnummer'].isin(overlaping)]
            # if not df_new.empty:
            #     self.logger.info(
            #         f"Got {len(df)} companies from API call. Removing {len(overlaping)} that already exists in DB, adding {len(df_new)} new ones.")
            self._bq.to_bq(df = df,
                           table_name=table,
                           dataset_name = dataset,
                            if_exists='merge',
                           merge_on = ['organisasjonsnummer']
                            )

        starttime = datetime.now()
        self.logger.info(f"Fetching data for NACE codes: {nace_codes[:10]} and geo_type: {geo_type} with values: {geo_value}")

        combinations = []
        if nace_codes and geo_value:
            for nace in nace_codes:
                for geo in geo_value:
                    combinations.append((nace,geo))
        elif nace_codes:
            for nace in nace_codes:
                combinations.append((nace,None))
        else:
            for geo in geo_value:
                combinations.append((None,geo))

        for i in range(0,len(combinations),BATCH_SIZE):

            comb = combinations[i:i+BATCH_SIZE]
            tasks = [fetch_single(nace,geo) for nace,geo in comb]
            self.logger.debug(f'fetching combination: {comb}')

            batch_df = await process_batch(tasks)
            dataframes.extend(batch_df)

            total_companies += sum(len(df) for df in batch_df)
            self.logger.debug(f'Processed {total_companies} companies so far')

            if save_bq and total_companies>=SAVE_INTERVAL:
                if dataframes:
                    prep_save(dataframes)
                    dataframes = []
                    total_count += total_companies
                    self.logger.info(f"Fetched {total_count} so far...")
                    total_companies = 0

        if save_bq:
            if dataframes:
                prep_save(dataframes)
                total_count += total_companies

        self.logger.info(f'Task completed in {datetime.now() - starttime}, got {total_count} companies')


    async def fill_roles(self):
        query = '''SELECT DISTINCT(c.organisasjonsnummer)
                FROM `brreg.company_data` c
                LEFT JOIN `brreg.roles` r ON c.organisasjonsnummer = r.organisasjonsnummer
                WHERE r.organisasjonsnummer IS NULL
                '''
        orgs = self._bq.read_bq(query)
        orgs = orgs['organisasjonsnummer'].tolist()
        self.logger.info(f"Found {len(orgs)} companies without role data")
        if orgs:
            return await self.get_roles(orgs,save_bq=True)

    async def fill_financials(self,head=None, year : int = 2024):
        query = f'''SELECT c.organisasjonsnummer
        FROM `brreg.company_data` c
        WHERE NOT EXISTS (
            SELECT 1 FROM `brreg.financial` f
            WHERE f.virksomhet_organisasjonsnummer = c.organisasjonsnummer
            AND f.regnskapsperiode_fraDato = "{year}-01-01"
        )
        '''
        if head:
            query += f'LIMIT {head}'
        orgs = self._bq.read_bq(query)
        orgs = orgs['organisasjonsnummer'].tolist()
        self.logger.info(f"Found {len(orgs)} companies without financial data")
        if orgs:
            return await self.get_financial_data(org_nums=orgs,
                                                 save_bq=True)

    async def fill_companies(self,org_nums):
        await self.get_financial_data(org_nums,save_bq=True)
        await self.get_companies(org_nums,save_bq=True)
        await self.get_roles(org_nums,save_bq=True)

