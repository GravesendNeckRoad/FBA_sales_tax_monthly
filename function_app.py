from os import getenv

import azure.functions as func

from Utilities.TaxReports import TaxRevenueReportGenerator

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    """
    Generates tax/revenue report by U.S. state via the `TaxRevenueReportGenerator` class

    Parameters (URI):
        -`account_name`: The name of the account you wish to run the report for (must match key-vault and env-vars)
        -`start_date`: The start of the range for which you wish to run the report (default=entire previous month)
        -`end_date`: The start of the range for which you wish to run the report (default=entire previous month)

    Returns:
        -`func.HttpResponse`: Confirmation or failure message
        -`None`: Uploads finished .xlsx report to specified blob container

    Considerations:
        -Review `Utils.report_tools.GenerateFBAReport` docstring for full list of run requirements 
         TODO: move the docstring somewhere more accessible
    """   
    # unpack query params 
    start_date = req.params.get('start_date', None)  # optional
    end_date = req.params.get('end_date', None)  # optional
    account_name = req.params.get('account_name')  # required
    if not account_name:
        return func.HttpResponse("Missing 'account_name' parameter in the URI", status_code=400)

    # generate report (no retries/error-handling here - everything is happening under the hood, inside the class)
    report_generator = TaxRevenueReportGenerator(account_name=account_name, start_date=start_date, end_date=end_date)
    report_generator.generate_tax_report(
        storage_account=getenv('STORAGE_ACCOUNT_NAME'), 
        container_name=getenv('TAX_REPORTS_BLOB_CONTAINER_NAME'),
        max_retries=10
        )

    return func.HttpResponse(
        f"Successfully processed report '{report_generator.report_name}.xlsx' and saved to blob container!",
        status_code=200
        )        
