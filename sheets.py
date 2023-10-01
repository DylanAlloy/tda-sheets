from tda.auth import *
from tda.streaming import StreamClient
import asyncio
from datetime import datetime
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json, os

def TDA(api_key, acct_id, contracts, sheet):
  # this is your tda token, rename if you want but change this if you do
  client = tda.auth.client_from_token_file('./tda_token.json',api_key)
  stream_client = StreamClient(client, account_id=acct_id)
  async def read_stream():
      await stream_client.login()
      await stream_client.quality_of_service(StreamClient.QOSLevel.SLOW)

      def print_message(data):
        _ = json.dumps(data['content'])
        _ = json.loads(_)
        print(_)
        try:
            sheet.update_values('DATA!A2:AZ500', _, contracts, sheet)
        except Exception as e:
            print(e)

      stream_client.add_level_one_option_handler(print_message)
      await stream_client.level_one_option_subs(list(contracts.keys()))

      while True:
          await stream_client.handle_message()

  asyncio.run(read_stream())

class Sheet:
    def __init__(self, s_id):
        self.s_id = s_id
        self.scopes = ['https://www.googleapis.com/auth/spreadsheets']
    def login(self):
        creds = None
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', self.scopes)
            self.creds = creds
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                # this is your sheets token, rename if you want but change this if you do
                flow = InstalledAppFlow.from_client_secrets_file(
                    'sheets.json', self.scopes)
                creds = flow.run_local_server(port=0)
                self.creds = creds
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
    def get_values(self, s_range):
        try:
            service = build('sheets', 'v4', credentials=self.creds)

            result = service.spreadsheets().values().get(
                spreadsheetId=self.s_id, range=s_range).execute()
            rows = result.get('values', [])
            print(f"{len(rows)} rows retrieved")
            return result
        except HttpError as error:
            print(f"An error occurred: {error}")
            return error
    def clear_values(self):
        service = build('sheets', 'v4', credentials=self.creds)
        body = {
            'values': []
        }
        service.spreadsheets().values().update(
            spreadsheetId=self.s_id, range='DATA!A2:AZ500',
            valueInputOption='USER_ENTERED', body=body).execute()
    def update_values(self, s_range, values, contracts, _sheet):
        try:
            current = self.get_values(s_range+str(len(contracts)+1))
            current = list([x for x in current['values']])
        except Exception as e:
            print(e)
        low_beast_count = _sheet.get_values("DATA!D2:A500")
        high_beast_count = _sheet.get_values("DATA!E2:A500")
        for contract, contract_data in contracts.items():
            print(contract_data)
            for value in values:
                if contract == value['key']:
                    contract_data.update(value)
            try:
                if contract_data['MARK'] >= float(contract_data['u_beast']):
                    contract_data['u_count'] += high_beast_count[contract] + 1
                elif contract_data['MARK'] <= float(contract_data['l_beast']):
                    contract_data['l_count'] += low_beast_count[contract] + 1
            except Exception as e:
                print(e)
            contract_data['ts'] = str(datetime.now())
            contract_data['lower_diff'] = float(contract_data['LAST_PRICE']) - float(contract_data['l_beast'])
            contract_data['upper_diff'] = float(contract_data['LAST_PRICE']) - float(contract_data['u_beast'])

        # header = ['lower_diff', 'upper_diff', 'ts', 'l_beast', 'u_beast', 'l_count', 'u_count', 'symbol', 'key', 'delayed', 'assetMainType', 'cusip', 'DESCRIPTION', 'ASK_PRICE', 'LAST_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE', 'TOTAL_VOLUME', 'OPEN_INTEREST', 'VOLATILITY', 'QUOTE_TIME', 'TRADE_TIME', 'MONEY_INTRINSIC_VALUE', 'QUOTE_DAY', 'TRADE_DAY', 'EXPIRATION_YEAR', 'MULTIPLIER', 'DIGITS', 'OPEN_PRICE', 'ASK_SIZE', 'LAST_SIZE', 'NET_CHANGE', 'STRIKE_PRICE', 'CONTRACT_TYPE', 'UNDERLYING', 'EXPIRATION_MONTH', 'TIME_VALUE', 'EXPIRATION_DAY', 'DELTA', 'GAMMA', 'THETA', 'VEGA', 'RHO', 'SECURITY_STATUS', 'THEORETICAL_OPTION_VALUE', 'UNDERLYING_PRICE', 'UV_EXPIRATION_TYPE', 'MARK']
        upsert = [[str(v) for v in list(x.values())] for x in list(contracts.values())]
        all_rows = []
        for row in upsert:
            all_rows.append(row)
        try:
            service = build('sheets', 'v4', credentials=self.creds)
            values = all_rows
            body = {
                'values': values
            }
            result = service.spreadsheets().values().update(
                spreadsheetId=self.s_id, range=s_range+str(len(contracts)+1),
                valueInputOption='USER_ENTERED', body=body).execute()
            print(f"{result.get('updatedCells')} cells updated.")
            return result
        except HttpError as error:
            print(f"An error occurred: {error}")
            return error

# sheet id
# Random below as place holder - not actual information 
sheet = Sheet('')
sheet.login()
sheet.clear_values()
SYMBOL = sheet.get_values("INPUT!A2:A500")
LOWER_BEAST = sheet.get_values("INPUT!B2:B500")
UPPER_BEAST = sheet.get_values("INPUT!C2:C500")
inputs = {}
TDA_SYMBOLS = []
LOWERS, UPPERS = [], []
for x in SYMBOL['values']:
    symbol = x[0][1:5]
    date = x[0][5:11]
    strike = x[0][12:]
    TDA_SYMBOLS.append(f'{symbol}_{date[2:4]}{date[4:6]}{date[0:2]}{"C" if x[0][-5] == "C" else "P"}{strike}')
symbols = [x[0] for x in SYMBOL['values']]
contracts = [x[0] for x in SYMBOL['values']]
l_beasts = [x[0] for x in LOWER_BEAST['values']]
u_beasts = [x[0] for x in UPPER_BEAST['values']]
l_count = [x[0] for x in LOWER_BEAST['values']]
u_count = [x[0] for x in UPPER_BEAST['values']]
lower_diff = [x[0] for x in UPPER_BEAST['values']]
upper_diff = [x[0] for x in UPPER_BEAST['values']]
dates = [str(datetime.now()) for x in SYMBOL['values']]
for lowers, uppers, d, c, l_b, u_b, l_count, u_count, s, tda in zip(lower_diff, upper_diff, dates, contracts, l_beasts, u_beasts, l_count, u_count, symbols, TDA_SYMBOLS):
    inputs[tda] = {
            "lower_diff": lowers
            , "upper_diff": uppers
            , "date": d
            , "l_beast": l_b
            , "u_beast": u_b
            , "l_count": 0
            , "u_count": 0
            , "symbol": s
        }
# api key and account id
# Random below as place holder - not actual information 
TDA('', '', inputs, sheet)