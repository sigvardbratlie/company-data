from src.modules import EninApi,BRREGapi,RateLimitException
from src.nace import *
import asyncio
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("--nace", type = str, nargs = "+",help = "Gets companies based on NACE code. Use `.` to fetch all")
parser.add_argument("--geo", type = str,nargs = "+",help = "Gets all data based on geo location. Use `.` to fetch all")
parser.add_argument("--geo-type", type = str, help = "Specify geotype")
parser.add_argument("--financial", type = str, help = "Gets companies based on NACE code.")
parser.add_argument("--get_all", action="store_true", help = "Gets companies based on NACE code.")
parser.add_argument("--fill",type = str, help = "Gets companies based on NACE code.")
#group = parser.add_mutually_exclusive_group(required = True)


args = parser.parse_args()





if __name__ == "__main__":
    async def main():
        brreg = BRREGapi()
        try:
            if args.nace or args.geo:
                if args.nace == ["."] or args.geo  == ["."]: #HENTER ALLE NACE KODER
                        nace_codes = brreg.bq.read_bq("""
                                                SELECT code
                                                FROM `company-data-455309.brreg.nace_codes`
                                                WHERE code LIKE "%.%"
                                                """)
                        nace_codes = nace_codes.code.tolist()
                        for i in nace_codes:
                            if i in over_10k or i in oslo_over10k:
                                nace_codes.remove(i)

                        await brreg.get_by_nace_geo(nace_codes=nace_codes,
                                                    save_interval=10000,
                                                    save_bq=True)
                        await brreg.get_by_nace_geo(nace_codes = over_10k,
                                                    geo_type='kommunenummer',
                                                    geo_value=kommune,
                                                    save_interval=100000,
                                                    save_bq=True)
                        await brreg.get_by_nace_geo(nace_codes = oslo_over10k,
                                                    geo_type='postnummer',
                                                    geo_value= oslo_postnr,
                                                    save_interval=100000,
                                                    save_bq=True)
                elif args.nace and args.geo:
                    if not args.geo_type:
                        raise TypeError(f'geo-type must be present when --geo flag is active')
                    await brreg.get_by_nace_geo(nace_codes=args.nace,
                                                geo_type=args.geo_type,
                                                geo_value=args.geo,
                                                save_interval=100000,
                                                save_bq=True)
                elif args.nace and not args.geo:
                    await brreg.get_by_nace_geo(nace_codes=args.nace,
                                            save_interval=100000,
                                            save_bq=True)

            #await brreg.fill_financial()
            #await brreg.fill_roles()

        except RateLimitException as e:
            print(f"rate limit exceeded. {e}")
        finally:
            await brreg.close()


    asyncio.run(main())