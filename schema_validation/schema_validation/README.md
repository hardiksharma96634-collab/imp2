# Welcome to Schema Validation Template
<font size="3">The template is design for the DataOS Data Products team for their investigations on schema validation issues for NPI or CPE.</font>

## How to use?
<font size="3"><strong>Step-1: </strong> Select/fill the appropriate required values from the widgets given above.<br></font>

<font size="3"><strong>Step-2: </strong>To start the investigation on required schema error issue, call the following function. This function will provide you the Investigation details(Originator Gun, Event Gun, Stack, Investigated range),Total events count, error events count and reproducibility percentage of issue for all the product families you have selected from widgets for the respective event:<br>
**schemaTemplate.processValidation()**<br></font>

<font size="3"><strong>Step-3: </strong>To print the reports of schema error events for all product families one by one, call following function:<br>
**schemaTemplate.utils.display_reports(display)**<br></font>

<font size="3"><strong>Step-4: </strong>To print the payloads(expected payloads(if any), error payloads(if events found with filterType of inclusion), compare payloads(if expected payload found for the same device reporting error events )) of schema error events for all product families one by one, call following function:<br>**schemaTemplate.utils.display_reports(display)**<br></font>
