# google_cloud_utils.py
from playwright.sync_api import *
import logging
import os
import datetime
from GCPLibrary import *


class UC4InformaticaHelper(GoogleCloudHelper):

    def __init__(self, playwright: Playwright):
        super().__init__(playwright)
    def login_UC4_and_navigate_to_homepage(self,page: Page, intClientID: str, strUsername: str, strPassword: str):
        page.locator("ecc-spinner").get_by_role("textbox").fill(intClientID)
        page.locator("ecc-form-field").filter(has_text="Name").get_by_role("textbox").fill(strUsername)
        page.get_by_role("textbox", name="(optional)").fill("WOW")
        page.locator("input[type=\"password\"]").fill(strPassword)
        page.locator("input[type=\"password\"]").press("Enter")
        page.wait_for_timeout(3000)

    def search_uc4_Job_and_execute(self,page: Page, strJobName: str):
        strJobStatus = None
        try:
            page.get_by_role("textbox", name="Search").fill(strJobName)
            page.get_by_role("button", name="Result Total").click()
            page.wait_for_timeout(3000)
        except Exception as e:
            print(f"Error in search_uc4_Job_and_execute: {e}")
        if page.get_by_text(strJobName, exact=True).is_visible():
            page.get_by_text(strJobName, exact=True).dblclick()
            page.wait_for_timeout(3000)
            page.get_by_role("button", name="Execute").click()
            page.get_by_label("This content is announced").get_by_role("button", name="Execute").click()
            strJobStatus = "Pass"
        else:
            strJobStatus = "Fail"
        page.wait_for_timeout(3000)
        return strJobStatus
    def verify_Uc4_Job_execution_status(self,page: Page):
        strJobVerificationStatus = None
        strActualJobEndTime = None
        try:
            page.get_by_role("button", name="Executions").click()
            page.wait_for_timeout(5000)
            # Locate all table rows in the page
            lisTablerows = page.locator("table >> tbody >> tr")
            # Locate the header row
            header_row = page.locator(".v-table-header-cell")
            header_cells = header_row.locator(".v-table-caption-container")
            if lisTablerows.count() > 0:
                # Find the index of the column with filters for fetching file Name
                for intHeaderRowIterator in range(header_cells.count()):
                    if header_cells.nth(intHeaderRowIterator).inner_text() == "RunID":
                        strRunIDIndex = intHeaderRowIterator + 1  # Add 1 for CSS nth-child indexing
                        strRunID = lisTablerows.nth(1).locator(f"td:nth-child({strRunIDIndex})").inner_text()
                    if header_cells.nth(intHeaderRowIterator).inner_text() == "Status":
                        str_StatusColumnIndex = intHeaderRowIterator + 1  # Add 1 for CSS nth-child indexing
                        strJobStatus = lisTablerows.nth(1).locator(f"td:nth-child({str_StatusColumnIndex})").inner_text()
                    if header_cells.nth(intHeaderRowIterator).inner_text() == "End Time":
                        str_EndTimeColumnIndex = intHeaderRowIterator + 1
                        break
                if strJobStatus == "Active"   :
                    while strJobStatus == "Active":
                        page.locator("div:nth-child(9) > .uc4_common_button").click()
                        page.wait_for_timeout(10000)
                        for intRunIDIterator in range(lisTablerows.count()):
                            if strRunID in lisTablerows.nth(intRunIDIterator).locator(f"td:nth-child({strRunIDIndex})").inner_text():
                                strJobStatus = lisTablerows.nth(intRunIDIterator).locator(f"td:nth-child({str_StatusColumnIndex})").inner_text()
                                strJobEndTime = lisTablerows.nth(intRunIDIterator).locator(f"td:nth-child({str_EndTimeColumnIndex})").inner_text()
                                break
                    strJobVerificationStatus = "Pass"
                elif strJobStatus == "ENDED_OK - ended normally":
                    strJobEndTime = lisTablerows.nth(1).locator(f"td:nth-child({str_EndTimeColumnIndex})").inner_text()
                    strJobVerificationStatus = "Pass"
                else:
                    strJobVerificationStatus = "FAIL"
                strJobEndTime_date = datetime.datetime.strptime(strJobEndTime, "%m/%d/%Y %H:%M")
                strActualJobEndTime = strJobEndTime_date.strftime("%Y%m%d%H%M")[:-1]
                return strJobVerificationStatus, strActualJobEndTime
        except Exception as e:
            print(f"Error in verify_Uc4_Job_execution_status: {e}")

