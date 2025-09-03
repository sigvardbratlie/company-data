from src.modules import EninApi,BRREGapi,RateLimitException
from src.nace import *
import asyncio
import argparse
import json

parser = argparse.ArgumentParser()

parser.add_argument("--nace", type = str, nargs = "+",help = "Gets companies based on NACE code. Use `.` to fetch all")
parser.add_argument("--geo", type = str,nargs = "+",help = "Gets all data based on geo location. Use `.` to fetch all")
parser.add_argument("--geo-type", type = str, help = "Specify geotype")
#parser.add_argument("--financial", type = str, help = "Gets companies based on NACE code.")
parser.add_argument("--get-all", action="store_true", help = "Gets companies based on NACE code.")


group_data = parser.add_mutually_exclusive_group(required = True)
group_data.add_argument("--companies", action = "store_true", help = "")
group_data.add_argument("--roles", action = "store_true", help = "")
group_data.add_argument("--financial", action = "store_true", help = "")

group_type  = parser.add_mutually_exclusive_group(required = True)
group_type.add_argument("--org-nr",action = "store_true", help = "")
group_type.add_argument("-u","--update",action = "store_true", help = "")
group_type.add_argument("-f","--fill",action = "store_true", help = "")

parser.add_argument("-a", "--all",action = "store_true", help = "")







if __name__ == "__main__":
    async def main():
        args = parser.parse_args()
        brreg = BRREGapi()
        try:
            if args.companies:
                if args.update and args.all:
                    async def get_all_companies(brreg):
                        query = """
                                SELECT code \
                                FROM brreg.nace_codes
                                WHERE code LIKE '%.%' \
                                """
                        nace = brreg.bq.read_bq(query)
                        nace_list = nace.code.tolist()
                        if len(nace_list)>0:
                            await brreg.get_items(inputs=nace_list,
                                                  fetcher=brreg.get_nace,
                                                  transformer=brreg.transform_pages,
                                                  saver=brreg.save_pages,
                                                  concurrent_requests=15,
                                                  save_interval=1000
                                                  )
                        knr = brreg.bq.read_bq("SELECT DISTINCT kommunenummer FROM brreg.geo_norge WHERE LOWER(kommunenavn) != 'oslo' ")
                        knr_list = knr.kommunenummer.tolist()
                        args_knr = [("00.000",knr) for knr in knr_list]
                        await brreg.get_items(inputs=args_knr,
                                                fetcher=brreg.get_nace_municipality,
                                                transformer=brreg.transform_pages,
                                                saver=brreg.save_pages,
                                                concurrent_requests=15,
                                                save_interval=5000
                                                )

                        args_oslo = [(nace,"0301") for nace in nace_list]
                        await brreg.get_items(inputs=args_oslo,
                                              fetcher=brreg.get_nace_municipality,
                                              transformer=brreg.transform_pages,
                                              saver=brreg.save_pages,
                                              concurrent_requests=15,
                                              save_interval=5000
                                              )


                        pnr = brreg.bq.read_bq("SELECT DISTINCT postnummer FROM brreg.geo_norge WHERE LOWER(kommunenavn) = 'oslo' ")
                        pnr_list = pnr.postnummer.tolist()
                        args_pnr = [("00.000",pnr) for pnr in pnr_list]
                        await brreg.get_items(inputs=args_pnr,
                                              fetcher=brreg.get_nace_postal,
                                              transformer=brreg.transform_pages,
                                              saver=brreg.save_pages,
                                              concurrent_requests=15,
                                              save_interval=1000
                                              )



                        ## ==== FETCH BY founding date ==========
                        # with open("exceeds_limit.json", "r") as f:
                        #     data = json.load(f)
                        #     if data:
                        #         nace = data.get("nace", {})
                        #         if nace:
                        #             args = []
                        #             for code in nace:
                        #                 args.append((code, "fra"))
                        #                 args.append((code, "til"))
                        #             await brreg.get_items(inputs=args,
                        #                                   fetcher=brreg.get_nace_date,
                        #                                   transformer=brreg.transform_pages,
                        #                                   saver=brreg.save_pages,
                        #                                   concurrent_requests=15,
                        #                                   save_interval=500
                        #                                   )

                        # ========= FETCH BY MUNICIPALITY AND NACE =========
                        # with open("exceeds_limit.json", "r") as f:
                        #     data = json.load(f)
                        #     if data:
                        #         nace = data.get("nace", {})
                        # knr = brreg.bq.read_bq("SELECT DISTINCT kommunenummer FROM brreg.geo_norge")
                        # knr_list = knr.kommunenummer.tolist()
                        # args = []
                        # for code in nace:
                        #     for knr in knr_list:
                        #         args.append((code, knr))
                        # await brreg.get_items(inputs=args,
                        #                       fetcher=brreg.get_nace_municipality,
                        #                       transformer=brreg.transform_pages,
                        #                       saver=brreg.save_pages,
                        #                       concurrent_requests=15,
                        #                       save_interval=5000
                        #                       )

                    await get_all_companies(brreg)

                    # knr = brreg.bq.read_bq("SELECT DISTINCT kommunenummer FROM brreg.geo_norge")
                    # knr_list = knr.kommunenummer.tolist()
                    # await brreg.get_items(inputs=knr_list,
                    #                             fetcher=brreg.get_page_by_municipality,
                    #                             transformer=brreg.transform_pages,
                    #                             saver=brreg.save_pages,
                    #                             concurrent_requests=15,
                    #                             save_interval=150
                    #                             )
                    #
                    # with open("exceeds_limit.json", "r") as f:
                    #     data = json.load(f)
                    #     if data:
                    #         kommunenummer = data.get("kommunenummer",{})
                    # if kommunenummer:
                    #     postnr = brreg.bq.read_bq(f"SELECT postnummer FROM brreg.geo_norge WHERE kommunenummer IN {tuple(kommunenummer.keys())}")
                    #     postnr_list = postnr.postnummer.tolist()
                    #     await brreg.get_items(inputs=postnr_list,
                    #                                 fetcher=brreg.get_page_by_postal_code,
                    #                                 transformer=brreg.transform_pages,
                    #                                 saver=brreg.save_pages,
                    #                                 concurrent_requests=15,
                    #                                 save_interval=500
                    #                                 )
            elif args.org_nr:
                await brreg.get_items(inputs=list(args.org_nr),
                                      fetcher=brreg.get_company,
                                      #transformer=brreg.transform_pages,
                                      #saver=brreg.save_pages,
                                      concurrent_requests=15,
                                      save_interval=100
                                      )

            if args.roles:
                if args.update or args.fill:
                    if args.update:
                        query = """
                        SELECT organisasjonsnummer
                        FROM brreg.companies
                        WHERE DATE_DIFF(CURRENT_DATE(),fetch_date,DAY) > 50
                        """
                    elif args.fill:
                        query = """
                            SELECT organisasjonsnummer
                            FROM brreg.companies c
                            WHERE NOT EXISTS (SELECT 1
                                              FROM brreg.roles r
                                              WHERE r.organisasjonsnummer = c.organisasjonsnummer)
                                """
                    else:
                        raise TypeError(f'Expected --fill or --update')
                    orgnums = brreg.bq.read_bq(query)
                    orgnums_list = orgnums.organisasjonsnummer.tolist()
                    await brreg.get_items_with_ids(orgnums_list,
                                                   fetcher=brreg.get_role,
                                                   transformer=brreg.transform_roles,
                                                   saver=brreg.save_roles,
                                                   concurrent_requests=30,
                                                   save_interval=50000)
                elif args.org_nr:
                    await brreg.get_items_with_ids(list(args.org_nr),
                                                   fetcher=brreg.get_role,
                                                   transformer=brreg.transform_roles,
                                                   saver=brreg.save_roles,
                                                   concurrent_requests=30,
                                                   save_interval=50000)

        except RateLimitException as e:
            print(f"rate limit exceeded. {e}")
        finally:
            await brreg.close()


    asyncio.run(main())