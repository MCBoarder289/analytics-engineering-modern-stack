To create a Metabase dashboard with manager-level metrics and a drill-down into agent-level details, you can use Custom Click Behavior or Linked Filters, depending on whether you want to use the Query Builder or SQL for your queries. 
Here is the step-by-step approach: 
1. Create the Data Questions (Summary & Detail) [1]  
First, ensure you have saved two separate questions in Metabase using the Query Builder (preferred for easiest drill-down) or SQL: 

• Question A (Summary): Pre-calculated manager table (e.g., Manager, Metric 1, Metric 2). 
• Question B (Detail): Pre-computed agent table (e.g., Agent, Manager, Metric 1, Metric 2). 

	• Tip: Ensure the Manager column exists in both tables to link them. [3]  

2. Build the Main Dashboard 

   1. Create a New Dashboard. 
   2. Add Question A (Summary) to the dashboard. 
   3. Add a Dashboard Filter (e.g., a "Manager" dropdown). 
   4. Wire the filter to the "Manager" column in Question A. [4, 5]  

3. Setup the Drill-Down Functionality [6]  
There are two main ways to make Question A drill into Question B: 

* Method A: Custom Click Behavior (Recommended) 1. On your dashboard, click Edit (pencil icon). 
  1. Click on Question A (Summary) and select Click behavior. 
  2. Select Go to a custom destination -&gt; Dashboard (create a second "Detail" dashboard) or Saved Question (select Question B). 
  3. Configure the click behavior to pass the manager name from the clicked row as a parameter to the second question/dashboard. 
  4. Result: Clicking a manager's row in table A opens table B filtered for that manager. [7, 8, 9, 10, 11]  

* Method B: Linked Filters (Best for Single Dashboard Experience) 1. Add both Question A and Question B to the same dashboard. 
  1. Set up the Manager Filter for Question A as done in Step 2. 
  2. Link the filter : In the dashboard editor, edit the Manager filter and choose "Linked filters". 
  3. Set the filter to control the "Manager" column in Question B as well. 
  4. Result: Selecting a manager in the filter updates both the Summary (A) and Detail (B) tables simultaneously. [12, 13]  

4. Optimize 

• Using SQL: If using native SQL, make sure to use  for optional filters. 
• Drill-through: If you use the Query Builder for both, Metabase automatically enables "See this [data]" options when clicking table rows. [1, 14]  

Summary of Best Practices 

| Feature [15] | Use Case  |
| --- | --- |
| Click Behavior | Best for going from a summary dashboard to a detailed dashboard.  |
| Linked Filters | Best for updating detailed tables on the same screen.  |
| Models | Use to pre-join or clean data before making questions.  |

AI responses may include mistakes.

[1] https://www.metabase.com/learn/metabase-basics/overview/tour-of-metabase
[2] https://www.metabase.com/case-studies/dribbble
[3] https://hydrolix.io/blog/aggregation-best-practices/
[4] https://www.youtube.com/watch?v=PO1WLfW9y_U
[5] https://www.metabase.com/learn/metabase-basics/querying-and-dashboards/dashboards/linking-filters
[6] https://www.metabase.com/learn/metabase-basics/querying-and-dashboards/questions/drill-through
[7] https://www.metabase.com/docs/latest/exploration-and-organization/exploration
[8] https://cljdoc.org/d/metabase-core/metabase-core/1.0.0-SNAPSHOT/doc/customizing-drill-through
[9] https://www.metabase.com/docs/latest/dashboards/multiple-series
[10] https://www.metabase.com/docs/latest/dashboards/interactive
[11] https://community.bmc.com/s/article/BMC-Helix-Dashboards-How-to-drill-down-using-time-ranges-from-dashboard-to-dashboard
[12] https://www.metabase.com/docs/latest/dashboards/linked-filters
[13] https://www.linkedin.com/posts/tanvir-taushif-751044204_linked-filters-in-metabase-a-game-changer-activity-7360627869099716609-0hPl
[14] https://www.metabase.com/features/drill-through
[15] https://www.metabase.com/learn/metabase-basics/overview/next-steps

