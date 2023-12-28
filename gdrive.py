"""Provides classes for interacting with Drive folders and spreadsheets.

This module is a wrapper for Google Drive API v3. It provides methods for
reading and writing to spreadsheets, downloading and uploading to folders,
creating new subfolders, etc. To use this module you must first create a 
Google service account and download your credentials.

Typical usage example:

  folder = DriveFolder("4Ho72rH29trDgD1GChZxTG4udQLjeKqnp", "My Folder")
  images = folder.download_files(["image/jpeg"])
  
  spreadsheet = Spreadsheet("4Ho72rH29trDgD1GChZxTG4udQLjeKqnp")
  subset = spreadsheet.readsheet("Sheet1", ((0,0), (3,4)))
"""

from __future__ import annotations
import os
import io
import sys
import mimetypes
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload

SCOPES = ["https://www.googleapis.com/auth/drive", 
          "https://www.googleapis.com/auth/spreadsheets"]

def authorize(scopes: list[str]) -> Credentials:
    """
    Authorizes Google Drive and Google Sheets API and saves user credentials.
    
    Parameters:
    scopes (list[str]): List of scopes to authorize (drive and sheets).

    Returns:
    Credentials: User credentials.
    """
    cred_path = 'credentials.json'
    token_path = 'token.json'
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, scopes)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(cred_path, scopes)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token_path, 'w') as token:
            token.write(creds.to_json())
    return creds

class DriveFolder:
    """
    Class for interacting with Google Drive folders.

    Attributes:
    creds (Credentials): User credentials.
    service (build): Google Drive API service.
    id (str): ID of the folder.
    name (str): Name of the folder.
    parent (DriveFolder): Parent folder.
    children (list[DriveFolder]): List of child folders.
        
    """
    def __init__(self, 
        folder_id: str, 
        name: str, 
        parent: DriveFolder = None
    ) -> None:
        """
        Initializes a DriveFolder object.

        Parameters:
        folder_id (str): ID of the folder.
        name (str): Name of the folder.
        parent (DriveFolder): Parent folder.
        """
        self.creds = authorize(SCOPES)
        self.service = build('drive', 'v3', credentials=self.creds)
        self.id = folder_id
        self.name = name
        self.parent = parent
        self.children = {}
        folders = self.list_files(["application/vnd.google-apps.folder"])
        for f in folders:
            self.children[f["name"]] = DriveFolder(f["id"], f["name"], self)
    
    def __repr__(self) -> str:
        """
        Returns a string representation of the DriveFolder object.
        """
        return f"name: {self.name}, id: {self.id}"
    
    def folder_structure(self, level: int = 0) -> str:
        """
        Returns a string representation of the folder structure.

        Parameters:
        level (int): Level of the folder in the structure.

        Returns:
        ret (str): String representation of the folder structure.
        """
        ret = "\t" * level + self.name + "\n"
        for c in self.children:
            ret += self.children[c].folder_structure(level + 1)
        return ret
    
    def list_files(self, mimetypes: list[str] =[]) -> list[dict] | None:
        """
        Lists all files in the folder.

        Parameters:
        mimetypes (list[str]): List of mimetypes to filter by.

        Returns:
        files (list[dict] | None): List of files in the folder.
        """
        page_token = None
        files = []
        query = f"'{self.id}' in parents and trashed=false"
        if mimetypes:
            query += f" and (mimeType='{mimetypes[0]}'"
            for mimetype in mimetypes[1:]:
                query += f" or mimeType='{mimetype}'"
            query += ")"
        try:
            while True:
                # pylint: disable=maybe-no-member
                request = self.service.files().list(
                    supportsAllDrives='true',
                    includeItemsFromAllDrives='true',
                    q=query,
                    spaces='drive',
                    fields='nextPageToken, files(id, name)', 
                    pageToken=page_token
                    )
                result = request.execute()
                files.extend(result.get('files', []))
                page_token = result.get('nextPageToken', None)
                if page_token is None:
                    break
        except HttpError as error:
            print(F'An error occurred: {error}')
            files = None
        return files
    
    def download_file(self, file_id: str) -> io.BytesIO | None:
        """
        Downloads a file from the folder.

        Parameters:
        file_id (str): ID of the file to download.

        Returns:
        file (io.BytesIO | None): File object.
        """
        try:
            # pylint: disable=maybe-no-member
            request = self.service.files().get_media(fileId=file_id)
            file = io.BytesIO()
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while done is False:
                _, done = downloader.next_chunk()
        except HttpError as error:
            print(F'An error occurred: {error}')
            file = None
        return file

    def download_files(self, mimetypes: list[str] =[]
    ) -> list[tuple[str, io.BytesIO]]:
        """
        Downloads files in the folder. If mimetypes is specified, only files 
        with the specified mimetypes will be downloaded. Otherwise, all files
        will be downloaded.

        Parameters:
        mimetypes (list[str]): List of mimetypes to filter by.

        Returns:
        files (list[tuple[str, io.BytesIO]]): List of tuples containing the
        file name and the file object.
        """
        files = []
        for file in self.list_files(mimetypes):
            data = self.download_file(file.get("id"))
            print(f"{file.get('name')} downloaded")
            files.append((file.get("name"), data))
        return files
    
    def upload_file(self, file_path: str, new_name: str = "") -> str | None:
        """
        Uploads a file to the folder.

        Parameters:
        file_path (str): Path to the file to upload.

        Returns:
        file_id (str | None): ID of the uploaded file if upload is successful.
        """
        if new_name:
            name = new_name
        else:
            name = os.path.basename(file_path)
        file_metadata = {
            'name': name,
            'parents': [self.id]
        }
        mimetype = mimetypes.guess_type(file_path)[0]
        media = MediaFileUpload(file_path, mimetype=mimetype, resumable=True)        
        try:
            # pylint: disable=maybe-no-member
            request = self.service.files().create(
                body=file_metadata, 
                media_body=media,
                fields='id'
                )
            file = request.execute()
        except HttpError as error:
            print(F'An error occurred: {error}')
            return None
        return file.get("id")  
    
    def upload_files(self, file_paths: list[str]) -> list[str] | list[None]:
        """
        Uploads files to the folder.

        Parameters:
        file_paths (list[str]): List of paths to the files to upload.

        Returns:
        ids (list[str] | list[None]): List of IDs of the uploaded files if upload is
        successful.

        """
        ids = [self.upload_file(file_path) for file_path in file_paths]
        return ids
        
    def upload_files_from_dir(self, dir_path: str) -> list[str] | list[None]:
        """
        Uploads all files in a directory to the folder.

        Parameters:
        dir_path (str): Path to the directory containing the files to upload.

        Returns:
        ids (list[str] | list[None]): List of IDs of the uploaded files if upload is
        successful.
        """
        file_paths = []
        for f in os.listdir(dir_path):
            abs_path = os.path.join(dir_path, f)
            if os.path.isfile(abs_path):
                file_paths.append(abs_path)
        return self.upload_files(file_paths)
    
    def move_file_from_folder(self, file_id: str, new_folder: DriveFolder
    ) -> list[str] | None:
        """
        Moves a file from the current folder to a new folder.

        Parameters:
        file_id (str): ID of the file to move.
        new_folder (DriveFolder): Folder to move the file to.

        Returns:
        parents (list[str] | None): List of IDs of the parents of the file if
        move is successful.
        """
        try:
            # pylint: disable=maybe-no-member
            file = self.service.files().get(
                fileId=file_id, fields='parents').execute()
            assert self.id in file.get('parents')
            file = self.service.files().update(fileId=file_id, 
                                          addParents=new_folder.id,
                                          removeParents=self.id,
                                        fields='id, parents').execute()
            return file.get('parents')
        except HttpError as error:
            print(F'An error occurred: {error}')
            return None

    def create_subfolder(self, name: str) -> DriveFolder | None:
        """
        Creates a subfolder in the current folder.

        Parameters:
        name (str): Name of the subfolder to create.

        Returns:
        folder (DriveFolder | None): DriveFolder object of the created subfolder
        if creation is successful.
        """
        file_metadata = {
            'name': name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [self.id] 
        }
        try:
            # pylint: disable=maybe-no-member
            request = self.service.files().create(
                body=file_metadata, 
                fields='id'
                )
            file = request.execute()
        except HttpError as error:
            print(F'An error occurred: {error}')
            return None
        else:
            folder = DriveFolder(file.get("id"), name, self)
            self.children[name] = folder
            return folder

class Spreadsheet:
    """
    Class for interacting with Google Sheets spreadsheets.

    Attributes:
    creds (Credentials): User credentials.
    service (build): Google Sheets API service.
    id (str): ID of the spreadsheet.
    sheets (dict): Dictionary of sheets in the spreadsheet.
    """
    def __init__(self, spreadsheet_id: str) -> None:
        """
        Initializes a Spreadsheet object.

        Parameters:
        spreadsheet_id (str): ID of the spreadsheet.
        """
        self.creds = authorize(SCOPES)
        self.service = build('sheets', 'v4', credentials=self.creds)
        self.id = spreadsheet_id
        self.sheets = {}
        service = build('sheets', 'v4', credentials=self.creds)
        try:
            request = service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
                )
            result = request.execute()
            sheets = result.get("sheets", [])
        except HttpError as error:
            print(f"An error occurred: {error}")
        else:
            for sheet in sheets:
                name = sheet["properties"]["title"]
                id = sheet["properties"]["sheetId"]
                shape = (sheet["properties"]["gridProperties"]["rowCount"],
                        sheet["properties"]["gridProperties"]["columnCount"])
                self.sheets[name] = {"id": id, "shape": shape}
    
    @staticmethod
    def col_num_to_letter(num: int) -> str:
        """
        Converts a column number to a column letter.

        Parameters:
        num (int): Column number.

        Returns:
        letter (str): Column letter.
        """
        start = 0   #  it can start either at 0 or at 1
        letter = ''
        while num > 25 + start:   
            letter += chr(65 + int((num - start) / 26) - 1)
            num = num - (int((num - start) / 26)) * 26
        letter += chr(65 - start + (int(num)))
        return letter
    
    @staticmethod
    def sheets_range(cell_range: tuple[tuple[int, int], tuple[int, int]]
    ) -> str:
        """
        Converts a cell range to a Sheets range.

        Parameters:
        cell_range (tuple[tuple[int, int], tuple[int, int]]): Cell range.

        Returns:
        range_name (str): Sheets range.
        """
        start, end = cell_range 
        assert start[0] <= end[0]
        assert start[1] <= end[1]
        start_name = f"{Spreadsheet.col_num_to_letter(start[1])}{start[0]+1}"
        end_name = f"{Spreadsheet.col_num_to_letter(end[1])}{end[0]+1}"
        return start_name + ":" + end_name

    def read_sheet(self, sheet_name: str, cell_range: tuple[tuple[int, int], 
    tuple[int, int]] =()) -> dict | HttpError:
        """
        Reads a sheet from the spreadsheet.

        Parameters:
        sheet_name (str): Name of the sheet to read.
        cell_range (tuple[tuple[int, int], tuple[int, int]]): Cell range to read.

        Returns:
        result (dict | HttpError): Dictionary containing the values of the sheet
        if read is successful. HttpError object if read is unsuccessful.
        """
        range_name = f"'{sheet_name}'"
        if cell_range:
            range_name += f"!{Spreadsheet.sheets_range(cell_range)}"
        try:
            request = self.service.spreadsheets().values().get(
                spreadsheetId=self.id,
                range=range_name
                )
            result = request.execute()
            return result.get('values', [])
        except HttpError as error:
            print(f"An error occurred: {error}")
            return error
       

    def read_sheet_to_df(self, sheet_name: str, cell_range: tuple[tuple[int, int], 
    tuple[int, int]] =()) -> pd.DataFrame:
        """
        Reads a sheet from the spreadsheet and converts it to a DataFrame.

        Parameters:
        sheet_name (str): Name of the sheet to read.
        cell_range (tuple[tuple[int, int], tuple[int, int]]): Cell range to read.

        Returns:
        df (pd.DataFrame): DataFrame containing the values of the sheet
        if read is successful. HttpError object if read is unsuccessful.
        """
        lists = self.read_sheet(sheet_name, cell_range)
        return read.lists_to_df(lists)
    
    def write_to_sheet(self, sheet_name: str, values: list[list[str]], 
    start_cell: tuple[int, int]=(0,0)) -> dict | HttpError:
        """
        Writes values to a sheet in the spreadsheet.

        Parameters:
        sheet_name (str): Name of the sheet to write to.
        values (list[list[str]]): Values to write to the sheet.
        start_cell (tuple[int, int]): Starting cell to write to.

        Returns:
        result (dict | HttpError): Dictionary containing the values of the sheet
        if write is successful. HttpError object if write is unsuccessful.
        """
        end_cell = (start_cell[0] + len(values) - 1, start_cell[0] + len(values[0]) - 1)
        range_name = f"'{sheet_name}'!{Spreadsheet.sheets_range((start_cell, end_cell))}"
        body = {
            'values': values
        }
        try:
            request = self.service.spreadsheets().values().update(
                    spreadsheetId=self.id,
                    range=range_name,
                    valueInputOption="USER_ENTERED",
                    body=body
                    )
            result = request.execute()
            return result
        except HttpError as error:
            print(f"An error occurred: {error}")
            return error
    
    def write_df_to_sheet(self, sheet_name: str, values_df: pd.DataFrame, 
    start_cell: tuple[int, int]=(0,0)) -> dict | HttpError:
        """
        Writes values from a DataFrame to a sheet in the spreadsheet.

        Parameters:
        sheet_name (str): Name of the sheet to write to.
        values_df (pd.DataFrame): DataFrame containing the values to write to the sheet.
        start_cell (tuple[int, int]): Starting cell to write to.

        Returns:
        result (dict | HttpError): Dictionary containing the values of the sheet
        if write is successful. HttpError object if write is unsuccessful.
        """
        lists = read.df_to_lists(values_df)
        return self.write_to_sheet(sheet_name, lists, start_cell)
    
    def append_to_sheet(self, sheet_name: str, values: list[list[str]]
    ) -> dict | HttpError:
        """
        Appends values to a sheet in the spreadsheet.

        Parameters:
        sheet_name (str): Name of the sheet to append to.
        values (list[list[str]]): Values to append to the sheet.

        Returns:
        result (dict | HttpError): Dictionary containing the values of the sheet
        if append is successful. HttpError object if append is unsuccessful.
        """
        body = {
            'values': values
        }
        try:
            request = self.service.spreadsheets().values().append(
                    spreadsheetId=self.id,
                    range=sheet_name,
                    valueInputOption="USER_ENTERED",
                    body=body
                    )
            result = request.execute()
            return result
        except HttpError as error:
            print(f"An error occurred: {error}")
            return error
        
    def append_df_to_sheet(self, sheet_name: str, values_df: pd.DataFrame
    ) -> dict | HttpError:
        """
        Appends values from a DataFrame to a sheet in the spreadsheet.

        Parameters:
        sheet_name (str): Name of the sheet to append to.
        values_df (pd.DataFrame): DataFrame containing the values to append to the sheet.

        Returns:
        result (dict | HttpError): Dictionary containing the values of the sheet
        if append is successful. HttpError object if append is unsuccessful.
        """
        lists = read.df_to_lists(values_df)
        return self.append_to_sheet(sheet_name, lists)
