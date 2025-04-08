# google_cloud_utils.py
from playwright.sync_api import *
import logging
import os
import datetime
import re
from docx.shared import Inches
import GCP_BigQuery_Query_Library
def getUserId():
    return os.getlogin()



###################################################### Class and Functions related to intializing Playwright and Browser #####################
"""
                            A helper class to manage Google Cloud interactions using Playwright.
        Methods:
            launch_browser() -> Browser:
                Launches a persistent browser context using Chromium if not already launched.
                Returns the browser instance.
        """
##############################################################################################################################################
class GoogleCloudHelper:

    def __init__(self, playwright: Playwright, user_data_dir: str = None):
        self.playwright = playwright
        self.user_data_dir = user_data_dir
        self.browser = None  # Initialize browser instance here
    # To Initialize the browser
    def launch_browser(self) -> Browser:
        if self.browser is None:  # Check if the browser is already launched
            chromium = self.playwright.chromium
            self.browser = chromium.launch_persistent_context(
                channel="chrome",
                headless=False,  # Set to True if you don't need the UI
                user_data_dir=self.user_data_dir,
                args=["--start-maximized","--profile-directory=Default"],
                chromium_sandbox=False
            )
        return self.browser

###################################################### Class and Functions related to Big Query ##############################################
"""
                            A Helper class for interacting with Google BigQuery using Playwright.
    Methods:
    navigate_to_big_query_studio(page: Page)
        Navigates to the BigQuery Studio page.
    verify_table_exists(page: Page, table_name: str, project_name: str)
        Verifies if a specified table exists in BigQuery under the given project.
    run_and_verify_query(page: Page, query: str)
        Runs a SQL query in BigQuery and verifies if the results are available.
    """
##############################################################################################################################################
class BigQueryHelper(GoogleCloudHelper):
 
    def __init__(self, playwright: Playwright):
        super().__init__(playwright)

    def navigate_to_big_query_studio(self, page: Page):
            page.get_by_label("Navigation menu (.)").click()
            page.get_by_label("Pinned products", exact=True).get_by_label("BigQuery", exact=True).click()
    
    def verify_table_exists(self, page: Page, table_name: str, project_name: str):
        try:
            strExpectedDatasetName,strExpectedTableName = table_name.split(".")
            ClearInputElement = page.get_by_role("button", name="Clear input").is_visible()
            strVerifyTableStatus = None
            if ClearInputElement is not False:
                page.get_by_role("button", name="Clear input").click()
            # Search for the table
            page.get_by_label("Search BigQuery resources").fill(table_name)
            page.keyboard.press("Enter")
            page.wait_for_timeout(5000)
            expect(page.get_by_text(re.compile(r"Found \d+ results\."))).not_to_contain_text("Found 0 results")
            strResult = page.get_by_text(re.compile(r"Found \d+ results\.")).inner_text()
            lisCheckDigitValue = re.findall(r'\d+', strResult)
            if int(lisCheckDigitValue[0]) > 0:
                # Find all list elements under the tree
                list_elements = page.locator("xpath=//div[@class='item-container']").locator('li')
                # Iterate through each list element and get its inner text
                for intListelementsiterator in range(list_elements.count()):
                    list_element = list_elements.nth(intListelementsiterator)
                    inner_text = list_element.inner_text()
                    if inner_text == project_name:
                        strActualDataSet =  list_elements.nth(intListelementsiterator+1).inner_text()
                        strActualTableName = list_elements.nth(intListelementsiterator+2).inner_text()
                        if strActualDataSet == strExpectedDatasetName and strActualTableName == strExpectedTableName:
                            list_elements.nth(intListelementsiterator+2).dblclick()
                            page.get_by_role("tab", name="Details").click()
                            objTableInfoDetails = page.get_by_label("Details", exact=True).locator("div").filter(has_text="Table info Edit details Table").nth(1)
                            strVerifyTableStatus = "PASS"
                            return strVerifyTableStatus, objTableInfoDetails
            else:
                strVerifyTableStatus = "FAIL"
                return strVerifyTableStatus
            page.wait_for_load_state("networkidle")
        except Exception as e:
            print(f"verify_table_exists '{table_name}': {e}")

    def fetch_tableinfo_from_bigquery_details(self, page: Page, table_name: str, project_name: str):
        try:
            strExpectedDatasetName, strExpectedTableName = table_name.split(".")
            list_elements = page.locator("xpath=//div[@class='item-container']").locator('li')
            # Iterate through each list element and get its inner text
            for intListelementsiterator in range(list_elements.count()):
                list_element = list_elements.nth(intListelementsiterator)
                inner_text = list_element.inner_text()
                if inner_text == project_name:
                    strActualDataSet = list_elements.nth(intListelementsiterator + 1).inner_text()
                    strActualTableName = list_elements.nth(intListelementsiterator + 2).inner_text()
                    if strActualDataSet == strExpectedDatasetName and strActualTableName == strExpectedTableName:
                        list_elements.nth(intListelementsiterator + 2).dblclick()
                        page.get_by_role("tab", name="Details").click()

        except Exception as e:
            print(f"fetch_tableinfo_from_bigquery_details '{table_name}': {e}")

    def run_and_verify_query(self, page: Page, query: str):
        try:
            strqueryResult = None
             # To close the Open Tab
            page.get_by_label("Open Welcome tab menu").click()
            strCloseTabElement = page.get_by_role("menuitem", name="Close other tabs").is_enabled()
            if strCloseTabElement is not False:
                page.get_by_role("menuitem", name="Close other tabs").click()
            page.wait_for_timeout(3000)
            strClosePopupElement = page.get_by_role("button", name="Close", exact=True).is_visible()
            #To Close Popups of unsaved works
            if strClosePopupElement is not False:
                page.get_by_role("button", name="Close", exact=True).click()
            page.keyboard.press("Escape")
            page.locator("button[name=\"addTabButton\"]").click()
            page.get_by_role("textbox", name="SQL editor;Press Alt+F1 for").clear()
            page.get_by_role("textbox", name="SQL editor;Press Alt+F1 for").fill(query)
            page.get_by_role("button", name="Run", exact=True).click()
            page.wait_for_timeout(5000)
            while page.get_by_role("tab", name="Results").is_enabled() is False:
                page.wait_for_timeout(5000)
            if page.get_by_role("tab", name="Results").is_enabled() is False:
                strqueryResult = "FAIL"
            else:
                strqueryResult = "PASS"
            return strqueryResult
        except Exception as e:
            print(f"run_and_verify_query '{query}': {e}")

###################################################### Class and Functions related to Cloud Storage ##########################################
"""
    A helper class for interacting with Google Cloud Platform (GCP) Cloud Storage using Playwright.

    Methods:
    navigate_to_gcp_cloud_storage(page: Page)
        Navigates to the GCP Cloud Storage section in the GCP console.

    GCP_Buckets_navigate_and_click_storage_bucket(page: Page, bucket_name: str)
        Navigates to the Cloud Storage Buckets section and clicks on the specified storage bucket.

    GCP_Buckets_Details_fetch_recent_file(page: Page, filter_column_name: str, inbound_file_name: str)
        Fetches the most recent file from the Cloud Storage bucket details based on the specified filter column and file name.
    """
##############################################################################################################################################
class CloudStorageHelper(GoogleCloudHelper):
    
    def __init__(self, playwright: Playwright):
        super().__init__(playwright)
    def navigate_to_gcp_cloud_storage(self, page: Page):
        page.get_by_label("Navigation menu (.)").click()
        page.get_by_label("Cloud Storage", exact=True).hover()
        page.get_by_role("menuitem", name="Buckets").click()
        page.wait_for_timeout(3000)
    def GCP_Buckets_navigate_and_click_storage_bucket(self, page: Page, bucket_name: str):
        try:
            strBucketVerificationStatus = " "
            # self.navigate_to_gcp_cloud_storage(page)
            if page.get_by_role("button", name="Clear filters").is_visible() is not False:
                page.get_by_role("button", name="Clear filters").click()
            if ";" in bucket_name:
                lisBucket_folders = bucket_name.split(";")
                page.get_by_role("combobox", name="Filter").fill(lisBucket_folders[0])
                page.keyboard.press("Enter")
                page.wait_for_timeout(3000)
                if page.get_by_role("link", name=lisBucket_folders[0], exact=True).is_visible() is False:
                    strBucketVerificationStatus = "FAIL"
                page.get_by_role("link", name=lisBucket_folders[0], exact=True).click()
                page.wait_for_timeout(3000)
                page.get_by_role("textbox", name="Filter").fill(lisBucket_folders[1])
                page.keyboard.press("Enter")
                page.wait_for_timeout(3000)
                if  page.locator(f"a:has-text('{lisBucket_folders[1]}')").is_visible() is False:
                    strBucketVerificationStatus = "FAIL"
                page.locator(f"a:has-text('{lisBucket_folders[1]}')").click()
                page.wait_for_timeout(3000)
            else:
                page.get_by_role("combobox", name="Filter").fill(bucket_name)
                page.keyboard.press("Enter")
                page.wait_for_timeout(3000)
                if page.get_by_role("link", name=bucket_name, exact=True).is_visible() is False:
                    strBucketVerificationStatus = "FAIL"
                page.get_by_role("link", name=bucket_name, exact=True).click()
                page.wait_for_timeout(3000)
            return strBucketVerificationStatus
        except Exception as e:
            print(f"GCP_Buckets_navigate_and_click_storage_bucket '{bucket_name}': {e}")
    def GCP_Buckets_Details_fetch_recent_file(self, page: Page, filter_column_name: str, inbound_file_name: str):
        try:
            strRecentFileName = " "
            BucketsDetailsFilterElement = page.get_by_role("button", name="Clear filters").is_visible()
            if BucketsDetailsFilterElement is not False:
                page.get_by_role("button", name="Clear filters").click()
            if page.get_by_role("combobox", name="Filter", exact=True).is_visible():
                page.get_by_role("combobox", name="Filter", exact=True).fill(inbound_file_name)
                page.keyboard.press("Enter")
                page.wait_for_timeout(3000)
            elif page.locator('div.cfctest-filter-basic-input[role="textbox"]').is_visible():
                page.locator('div.cfctest-filter-basic-input[role="textbox"]').fill(inbound_file_name)
                page.keyboard.press("Enter")
                page.wait_for_timeout(3000)
            # Locate all table rows in the page
            lisTablerows = page.locator("table >> tbody >> tr")
            # Locate the header row
            header_row = page.locator("tr.cfc-table-header-row")
            header_cells = header_row.locator("th")
            if lisTablerows.count() > 0:
            # Find the index of the column with filters for fetching file Name
                str_FilterColumnIndex = None
                for intHeaderRowIterator in range(header_cells.count()):
                    if header_cells.nth(intHeaderRowIterator).inner_text() == filter_column_name:
                        str_FilterColumnIndex = intHeaderRowIterator + 1  # Add 1 for CSS nth-child indexing
                        break
                strRandomDateTime = datetime.datetime(2020, 1, 1)
                # Iterate through each row
                for intTableRowIterator in range(lisTablerows.count()):
                    lisActualRow = lisTablerows.nth(intTableRowIterator)
                    strFilteredColumnDate = lisActualRow.locator(f"td:nth-child({str_FilterColumnIndex + 1})").inner_text()  # Add 1 for skip the header row
                    strConvertedFilteredColumnDate = datetime.datetime.strptime(strFilteredColumnDate, '%d %b %Y, %H:%M:%S')
                    # Compare and update
                    if strConvertedFilteredColumnDate > strRandomDateTime:
                        strRandomDateTime = strConvertedFilteredColumnDate
                        strRecentFileName = lisActualRow.locator(f"td:nth-child({2})").inner_text()
            else:
                strRecentFileName = "FAIL"
            return strRecentFileName
            page.wait_for_load_state("networkidle")
        except Exception as e:
            print(f" Error - GCP_Buckets_Details_fetch_recent_file '{inbound_file_name}': {e}")

###################################################### Class and Functions related to Dag Composer ###########################################

##############################################################################################################################################
class DagComposerHelper(GoogleCloudHelper):
    def __init__(self, playwright: Playwright):
        super().__init__(playwright)
        self.dictDagId = {'de-nonpii-composer-uat': 'beda3746c736432ab1f6cb3fcf80f047'
                     ,'de-nonpii-composer-test': '36d7fc938e784938afbbd09cb26c9fd1'
                     ,'im-wdploads-composer-uat': '6a9bd9cbe88946059f98cc2098af88a6', 'imsploads-composer-uat': '0a5d0737e2af4f6d909698860bc4ea7f'}
    def Navigate_to_Dag_url_and_check_the_Dag_Status(self, page: Page, dag_composer_id: str, file_name: str ):
        try:
            strDagId = self.dictDagId.get(dag_composer_id)
            print(strDagId)
            strDagUrl = f"https://{strDagId}-dot-us-central1.composer.googleusercontent.com/dagrun/list/"
            page.goto(strDagUrl)
            try:
                page.locator(f'xpath=(//div[@role="link" and @class="VV3oRb YZVTmd SmR8"])[1]').click()
                page.get_by_role("button", name="Continue").click()
            except Exception as e:
                print(e)
            page.get_by_role("link", name="Browse").click()
            page.get_by_role("link", name="DAG Runs").click()
            page.get_by_role("link", name="Search").click()
            page.get_by_role("button", name="Add Filter").click()
            page.get_by_role("link", name="Run Id", exact=True).click()
            page.get_by_placeholder("Run Id").click()
            page.get_by_placeholder("Run Id").fill(file_name)
            page.get_by_role("button", name="Search ").click()
            page.locator(f'xpath=//table[@class="table table-bordered table-hover"]/tbody/tr[1]/td[6]').click()
            page.wait_for_load_state("networkidle")
            page.get_by_role("tab", name="Details").click()
            # expect(page.get_by_label("Details", exact=True).get_by_role("rowgroup")).to_contain_text("success")
            RowGroup = page.get_by_label("Details", exact=True).get_by_role("rowgroup")
            DagStatus = RowGroup.first.get_by_role("row").first
            DagStatus = DagStatus.text_content()
            DagStatus = DagStatus.replace('Status','')
            print(DagStatus)
            while True:
                if DagStatus == 'running':
                    page.wait_for_timeout(10000)
                    RowGroup = page.get_by_label("Details", exact=True).get_by_role("rowgroup")
                    DagStatus = RowGroup.first.get_by_role("row").first
                    DagStatus = DagStatus.text_content()
                    DagStatus = DagStatus.replace('Status','')
                else:
                    return DagStatus
        except Exception as e:
            print(f" Error - Navigate_to_Dag_url_and_check_the_Dag_Status ' : {e}")
        

class SchemaValidation(GoogleCloudHelper):

    def __init__(self, playwright: Playwright):
        super().__init__(playwright)

    def handle_dialog(self, dialog):
        alert_message = dialog.message
        dialog.accept()
        return alert_message
        

    def navigate_Schema_Validation_url(self, page : Page):
        page.goto(r'https://script.google.com/a/macros/tcs.woolworths.com.au/s/AKfycbwJeetFBsw2Ev6EoqbUXn7mc3bY3Lzdz7jQX2rELvjmZJ-A6pp76Ah9tpEMXltEPXpdlQ/exec')
        page.wait_for_load_state("networkidle")

    def VerifySchemaValidation(self, page : Page, mapping_doc_link :str, project_Id : str, table_name : str):
        self.navigate_Schema_Validation_url(page)
        dataset_name = table_name.split('.')[0]
        gcp_table_name = table_name.split('.')[1]
        page.locator("#sandboxFrame").content_frame.locator("#userHtmlFrame").content_frame.get_by_placeholder("Enter Your Google Sheet link ").click()
        page.locator("#sandboxFrame").content_frame.locator("#userHtmlFrame").content_frame.get_by_placeholder("Enter Your Google Sheet link ").fill(mapping_doc_link)
        with page.expect_event("dialog") as dialog_info:
            page.locator("#sandboxFrame").content_frame.locator("#userHtmlFrame").content_frame.get_by_role("button", name="Validate Link").click()
        dialog = dialog_info.value
        alert_message = dialog.message
        print(alert_message)
        dialog.accept()
        if alert_message != 'Valid Link':
            return "FAIL"
        page.locator("#sandboxFrame").content_frame.locator("#userHtmlFrame").content_frame.get_by_placeholder("Enter valid Project ID").click()
        page.locator("#sandboxFrame").content_frame.locator("#userHtmlFrame").content_frame.get_by_placeholder("Enter valid Project ID").fill(project_Id)
        page.locator("#sandboxFrame").content_frame.locator("#userHtmlFrame").content_frame.get_by_placeholder("Enter valid Dataset").click()
        page.locator("#sandboxFrame").content_frame.locator("#userHtmlFrame").content_frame.get_by_placeholder("Enter valid Dataset").fill(dataset_name)
        page.locator("#sandboxFrame").content_frame.locator("#userHtmlFrame").content_frame.get_by_placeholder("Enter valid Table Name").click()
        page.locator("#sandboxFrame").content_frame.locator("#userHtmlFrame").content_frame.get_by_placeholder("Enter valid Table Name").fill(gcp_table_name)
        with page.expect_event("dialog") as dialog_info:
            page.locator("#sandboxFrame").content_frame.locator("#userHtmlFrame").content_frame.get_by_role("button", name="Run the Query").click()
        dialog = dialog_info.value
        alert_message = dialog.message
        dialog.accept()
        if alert_message != 'Query Result Updated in the Sheet':
            return "FAIL"
        with page.expect_event("dialog") as dialog_info:
            page.locator("#sandboxFrame").content_frame.locator("#userHtmlFrame").content_frame.get_by_role("button", name="Compare Mapping Sheet and GCP").click()
        dialog = dialog_info.value
        alert_message = dialog.message
        dialog.accept()
        if alert_message == 'Data Compared and Updated in the Sheet- Status : Failed':
            return "FAIL"
        elif alert_message == 'Data Compared and Updated in the Sheet- Status : Passed':
            return 'PASS'
        else:
            return "FAIL"
        