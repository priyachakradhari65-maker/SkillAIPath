import gradio as gr
import pandas as pd
import os
from databricks.sdk import WorkspaceClient
from datetime import datetime, timedelta

# Initialize Databricks SDK - Apps V2 auto-configures from environment
# Environment provides: DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
w = WorkspaceClient()
current_user = w.current_user.me().user_name

def get_warehouse_id():
    """Get warehouse ID from env or auto-detect first available warehouse"""
    # 1. Check environment variable (set via app.yaml or deployment)
    warehouse_id = os.getenv("WAREHOUSE_ID")
    if warehouse_id:
        print(f"✅ Using warehouse from WAREHOUSE_ID env: {warehouse_id}")
        return warehouse_id
    
    # 2. Auto-detect first available warehouse
    try:
        warehouses = list(w.warehouses.list())
        if warehouses:
            warehouse_id = warehouses[0].id
            print(f"✅ Auto-detected warehouse: {warehouses[0].name} (ID: {warehouse_id})")
            return warehouse_id
        else:
            raise ValueError("No SQL warehouses available in this workspace")
    except Exception as e:
        raise ValueError(f"Failed to get warehouse: {str(e)}")

WAREHOUSE_ID = get_warehouse_id()

def query_uc(sql):
    """Execute SQL against Unity Catalog - READ operations"""
    try:
        result = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=sql,
            wait_timeout="30s"
        )
        
        if result.result and result.result.data_array:
            columns = [col.name for col in result.manifest.schema.columns]
            data = result.result.data_array
            return pd.DataFrame(data, columns=columns)
        else:
            return pd.DataFrame({"Message": ["No data available"]})
    except Exception as e:
        return pd.DataFrame({"Error": [str(e)]})

def execute_uc(sql):
    """Execute SQL against Unity Catalog - WRITE operations"""
    try:
        w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=sql,
            wait_timeout="30s"
        )
        return True, "✅ Data written successfully"
    except Exception as e:
        return False, f"❌ Error: {str(e)}"

# =====================================================================
# TAB 1: ML MODEL FEEDBACK & OVERRIDES
# =====================================================================

def get_churn_predictions():
    """Get current churn predictions from gold (direct model output, dev only)"""
    sql = "SELECT * FROM sap_dev.gold.churn_predictions ORDER BY churn_probability DESC"
    return query_uc(sql)

def get_recent_feedback():
    """Get recent model feedback"""
    sql = """
    SELECT 
        model_name as Model,
        entity_id as EntityID,
        feedback_type as FeedbackType,
        notes as Notes,
        created_by as SubmittedBy,
        created_at as SubmittedAt
    FROM sap_dev.gold.model_feedback
    ORDER BY created_at DESC
    LIMIT 50
    """
    return query_uc(sql)

def submit_model_feedback(model_name, entity_id, prediction_value, actual_outcome, feedback_type, notes):
    """Submit feedback on ML model prediction"""
    if not model_name or not entity_id or not feedback_type:
        return "❌ Please fill in Model Name, Entity ID, and Feedback Type"
    
    # Escape single quotes in notes
    notes_clean = notes.replace("'", "''") if notes else ""
    actual_clean = actual_outcome.replace("'", "''") if actual_outcome else ""
    
    sql = f"""
    INSERT INTO sap_dev.gold.model_feedback
    (feedback_id, model_name, entity_id, prediction_value, actual_outcome, feedback_type, notes, created_by, created_at)
    VALUES
    (ABS(CAST(RAND() * 1000000000 AS BIGINT)), '{model_name}', '{entity_id}', {prediction_value if prediction_value else 'NULL'}, 
     {f"'{actual_clean}'" if actual_outcome else 'NULL'}, '{feedback_type}', {f"'{notes_clean}'" if notes else 'NULL'}, 
     '{current_user}', CURRENT_TIMESTAMP())
    """
    
    success, msg = execute_uc(sql)
    if success:
        return f"✅ Feedback submitted successfully by {current_user}\n\nRefresh the 'Recent Feedback' tab to see your entry."
    return msg

def get_churn_overrides():
    """Get active churn prediction overrides"""
    sql = """
    SELECT 
        customer_id as CustomerID,
        original_churn_probability as OriginalProb,
        override_churn_probability as OverrideProb,
        reason as Reason,
        valid_from as ValidFrom,
        valid_until as ValidUntil,
        created_by as CreatedBy
    FROM sap_dev.gold.churn_prediction_overrides
    ORDER BY created_at DESC
    LIMIT 50
    """
    return query_uc(sql)

def override_churn_prediction(customer_id, original_prob, override_prob, reason, valid_days):
    """Override churn prediction with business judgment"""
    if not customer_id or not override_prob or not reason:
        return "❌ Please fill in Customer ID, Override Probability, and Reason"
    
    try:
        override_prob_float = float(override_prob)
        if not (0 <= override_prob_float <= 1):
            return "❌ Override probability must be between 0 and 1"
    except ValueError:
        return "❌ Invalid override probability - must be a number between 0 and 1"
    
    # Calculate valid_until timestamp
    if valid_days and int(valid_days) > 0:
        valid_until_ts = f"CURRENT_TIMESTAMP() + INTERVAL {int(valid_days)} DAYS"
    else:
        valid_until_ts = "NULL"
    
    reason_clean = reason.replace("'", "''")
    
    sql = f"""
    INSERT INTO sap_dev.gold.churn_prediction_overrides
    (override_id, customer_id, original_churn_probability, override_churn_probability, reason, valid_from, valid_until, created_by, created_at)
    VALUES
    (ABS(CAST(RAND() * 1000000000 AS BIGINT)), {int(customer_id)}, {float(original_prob) if original_prob else 'NULL'}, 
     {override_prob_float}, '{reason_clean}', CURRENT_TIMESTAMP(), {valid_until_ts}, '{current_user}', CURRENT_TIMESTAMP())
    """
    
    success, msg = execute_uc(sql)
    if success:
        return f"✅ Churn override created by {current_user}\n\nCustomer {customer_id} probability overridden to {override_prob}\nReason: {reason}\n\nRefresh 'Active Overrides' to see your entry."
    return msg

# =====================================================================
# TAB 2: INVENTORY ADJUSTMENTS
# =====================================================================

def get_inventory_status():
    """Get current inventory levels - FROM CACHE"""
    sql = """
    SELECT 
        ProductID,
        ProductName,
        Category,
        CurrentStock,
        ReorderLevel,
        WarehouseLocation
    FROM sap_dev.bi_center_cache.inventory_view
    ORDER BY CurrentStock ASC
    """
    return query_uc(sql)

def get_recent_adjustments():
    """Get recent inventory adjustments"""
    sql = """
    SELECT 
        product_id as ProductID,
        warehouse_id as WarehouseID,
        old_qty as SystemQty,
        new_qty as PhysicalQty,
        delta as Delta,
        reason_code as Reason,
        adjusted_by as AdjustedBy,
        adjusted_at as AdjustedAt
    FROM sap_dev.gold.inventory_adjustments
    ORDER BY adjusted_at DESC
    LIMIT 50
    """
    return query_uc(sql)

def submit_inventory_adjustment(product_id, warehouse_id, old_qty, new_qty, reason_code, location, notes):
    """Submit inventory adjustment after physical count"""
    if not product_id or not warehouse_id or old_qty is None or new_qty is None or not reason_code:
        return "❌ Please fill in Product ID, Warehouse ID, System Qty, Physical Count, and Reason Code"
    
    try:
        old_qty_int = int(old_qty)
        new_qty_int = int(new_qty)
        delta = new_qty_int - old_qty_int
    except ValueError:
        return "❌ Invalid quantity values - must be whole numbers"
    
    location_clean = location.replace("'", "''") if location else ""
    notes_clean = notes.replace("'", "''") if notes else ""
    
    sql = f"""
    INSERT INTO sap_dev.gold.inventory_adjustments
    (adjustment_id, product_id, warehouse_id, old_qty, new_qty, delta, reason_code, location, adjusted_by, adjusted_at, notes)
    VALUES
    (ABS(CAST(RAND() * 1000000000 AS BIGINT)), {int(product_id)}, {int(warehouse_id)}, {old_qty_int}, {new_qty_int}, {delta}, 
     '{reason_code}', {f"'{location_clean}'" if location else 'NULL'}, '{current_user}', CURRENT_TIMESTAMP(), 
     {f"'{notes_clean}'" if notes else 'NULL'})
    """
    
    success, msg = execute_uc(sql)
    if success:
        delta_str = f"+{delta}" if delta > 0 else str(delta)
        return f"✅ Inventory adjustment recorded by {current_user}\n\nProduct {product_id} at Warehouse {warehouse_id}\nSystem: {old_qty_int} → Physical: {new_qty_int} (Delta: {delta_str})\nReason: {reason_code}\n\nRefresh 'Recent Adjustments' to see your entry."
    return msg

# =====================================================================
# TAB 3: CUSTOMER PRIORITY FLAGS
# =====================================================================

def get_customers():
    """Get customer list"""
    sql = """
    SELECT 
        CustomerID,
        CustomerName,
        TotalSpend as TotalRevenue,
        TotalPurchases as TotalOrders,
        AvgOrderValue,
        DaysSinceLastPurchase as RecencyDays
    FROM sap_dev.gold.mart_customer_360
    ORDER BY TotalSpend DESC
    LIMIT 100
    """
    return query_uc(sql)

def get_priority_flags():
    """Get active customer priority flags"""
    sql = """
    SELECT 
        customer_id as CustomerID,
        priority_level as PriorityLevel,
        flag_type as FlagType,
        reason as Reason,
        valid_from as ValidFrom,
        valid_until as ValidUntil,
        created_by as CreatedBy,
        created_at as CreatedAt
    FROM sap_dev.gold.customer_priority_flags
    ORDER BY created_at DESC
    LIMIT 50
    """
    return query_uc(sql)

def set_customer_priority(customer_id, priority_level, flag_type, reason, valid_days, notes):
    """Set customer priority flag"""
    if not customer_id or not priority_level or not flag_type or not reason:
        return "❌ Please fill in Customer ID, Priority Level, Flag Type, and Reason"
    
    # Calculate valid_until timestamp
    if valid_days and int(valid_days) > 0:
        valid_until_ts = f"CURRENT_TIMESTAMP() + INTERVAL {int(valid_days)} DAYS"
    else:
        valid_until_ts = "NULL"
    
    reason_clean = reason.replace("'", "''")
    notes_clean = notes.replace("'", "''") if notes else ""
    
    sql = f"""
    INSERT INTO sap_dev.gold.customer_priority_flags
    (flag_id, customer_id, priority_level, flag_type, reason, valid_from, valid_until, created_by, created_at, notes)
    VALUES
    (ABS(CAST(RAND() * 1000000000 AS BIGINT)), {int(customer_id)}, '{priority_level}', '{flag_type}', '{reason_clean}', 
     CURRENT_TIMESTAMP(), {valid_until_ts}, '{current_user}', CURRENT_TIMESTAMP(), {f"'{notes_clean}'" if notes else 'NULL'})
    """
    
    success, msg = execute_uc(sql)
    if success:
        return f"✅ Customer priority flag set by {current_user}\n\nCustomer {customer_id} marked as '{priority_level}'\nFlag Type: {flag_type}\nReason: {reason}\n\nRefresh 'Active Flags' to see your entry."
    return msg

# =====================================================================
# TAB 4: DATA QUALITY ISSUES
# =====================================================================

def get_data_quality_issues():
    """Get reported data quality issues"""
    sql = """
    SELECT 
        table_name as TableName,
        column_name as ColumnName,
        issue_type as IssueType,
        severity as Severity,
        description as Description,
        status as Status,
        reported_by as ReportedBy,
        reported_at as ReportedAt
    FROM sap_dev.gold.data_quality_issues
    ORDER BY 
        CASE severity 
            WHEN 'critical' THEN 1
            WHEN 'high' THEN 2
            WHEN 'medium' THEN 3
            WHEN 'low' THEN 4
        END,
        reported_at DESC
    LIMIT 100
    """
    return query_uc(sql)

def report_data_quality_issue(table_name, column_name, issue_type, severity, description, sample_records):
    """Report a data quality issue"""
    if not table_name or not issue_type or not severity or not description:
        return "❌ Please fill in Table Name, Issue Type, Severity, and Description"
    
    table_clean = table_name.replace("'", "''")
    column_clean = column_name.replace("'", "''") if column_name else ""
    desc_clean = description.replace("'", "''")
    sample_clean = sample_records.replace("'", "''") if sample_records else ""
    
    sql = f"""
    INSERT INTO sap_dev.gold.data_quality_issues
    (issue_id, table_name, column_name, issue_type, severity, description, sample_records, reported_by, reported_at, status, assigned_to, resolved_at, resolution_notes)
    VALUES
    (ABS(CAST(RAND() * 1000000000 AS BIGINT)), '{table_clean}', {f"'{column_clean}'" if column_name else 'NULL'}, 
     '{issue_type}', '{severity}', '{desc_clean}', {f"'{sample_clean}'" if sample_records else 'NULL'}, 
     '{current_user}', CURRENT_TIMESTAMP(), 'open', NULL, NULL, NULL)
    """
    
    success, msg = execute_uc(sql)
    if success:
        return f"✅ Data quality issue reported by {current_user}\n\nTable: {table_name}\nSeverity: {severity}\nStatus: Open\n\nData engineering has been notified. Refresh the issues list to see your entry."
    return msg

# =====================================================================
# TAB 5: DEMAND FORECAST ADJUSTMENTS
# =====================================================================

def get_demand_forecasts():
    """Get demand forecasts - FROM CACHE"""
    sql = """
    SELECT 
        ProductID,
        ProductName,
        ForecastDate,
        PredictedDemand as ForecastQty,
        ConfidenceLower,
        ConfidenceUpper
    FROM sap_dev.bi_center_cache.demand_forecast_view
    ORDER BY ForecastDate DESC, PredictedDemand DESC
    """
    return query_uc(sql)

def get_forecast_adjustments():
    """Get recent forecast adjustments"""
    sql = """
    SELECT 
        product_id as ProductID,
        forecast_date as ForecastDate,
        ml_forecast_qty as MLForecast,
        adjustment_factor as Factor,
        adjusted_forecast_qty as AdjustedForecast,
        reason as Reason,
        adjusted_by as AdjustedBy,
        adjusted_at as AdjustedAt
    FROM sap_dev.gold.demand_forecast_adjustments
    ORDER BY adjusted_at DESC
    LIMIT 50
    """
    return query_uc(sql)

def adjust_demand_forecast(product_id, forecast_date, ml_forecast_qty, adjustment_factor, reason, event_type, notes):
    """Adjust demand forecast based on business knowledge"""
    if not product_id or not forecast_date or ml_forecast_qty is None or not adjustment_factor or not reason:
        return "❌ Please fill in Product ID, Forecast Date, ML Forecast Qty, Adjustment Factor, and Reason"
    
    try:
        ml_qty = int(ml_forecast_qty)
        adj_factor = float(adjustment_factor)
        adjusted_qty = int(ml_qty * adj_factor)
    except ValueError:
        return "❌ Invalid numeric values - ML Forecast must be a whole number, Adjustment Factor must be a decimal (e.g., 1.5)"
    
    reason_clean = reason.replace("'", "''")
    event_clean = event_type.replace("'", "''") if event_type else ""
    notes_clean = notes.replace("'", "''") if notes else ""
    
    sql = f"""
    INSERT INTO sap_dev.gold.demand_forecast_adjustments
    (adjustment_id, product_id, forecast_date, ml_forecast_qty, adjustment_factor, adjusted_forecast_qty, reason, event_type, adjusted_by, adjusted_at, notes)
    VALUES
    (ABS(CAST(RAND() * 1000000000 AS BIGINT)), {int(product_id)}, DATE'{forecast_date}', {ml_qty}, {adj_factor}, {adjusted_qty}, 
     '{reason_clean}', {f"'{event_clean}'" if event_type else 'NULL'}, '{current_user}', CURRENT_TIMESTAMP(), 
     {f"'{notes_clean}'" if notes else 'NULL'})
    """
    
    success, msg = execute_uc(sql)
    if success:
        pct_change = int((adj_factor - 1) * 100)
        pct_str = f"+{pct_change}%" if pct_change > 0 else f"{pct_change}%"
        return f"✅ Demand forecast adjusted by {current_user}\n\nProduct {product_id} on {forecast_date}\nML Forecast: {ml_qty} → Adjusted: {adjusted_qty} ({pct_str})\nReason: {reason}\n\nRefresh 'Recent Adjustments' to see your entry."
    return msg

# =====================================================================
# GRADIO UI - PRODUCTION GRADE
# =====================================================================

with gr.Blocks(title="Business Intelligence Center", theme=gr.themes.Soft()) as demo:
    gr.Markdown("# Business Intelligence Center")
    gr.Markdown("**Collaborate on Data** | View Insights & Contribute Your Expertise")
    gr.Markdown(f"*Logged in as:* `{current_user}`")
    gr.Markdown("---")
    
    # TAB 1: ML Model Feedback
    with gr.Tab("AI Model Feedback"):
        gr.Markdown("### Submit Feedback on AI Predictions & Override When You Know Better")
        
        with gr.Row():
            with gr.Column(scale=1):
                gr.Markdown("#### View AI Churn Predictions")
                churn_df = gr.Dataframe(interactive=False, height=300)
                load_churn_btn = gr.Button("Load Predictions", variant="primary")
                load_churn_btn.click(get_churn_predictions, outputs=churn_df)
                
                gr.Markdown("#### Active Overrides")
                overrides_df = gr.Dataframe(interactive=False, height=250)
                load_overrides_btn = gr.Button("📋 Load Overrides", variant="secondary")
                load_overrides_btn.click(get_churn_overrides, outputs=overrides_df)
            
            with gr.Column(scale=1):
                gr.Markdown("#### Recent Feedback from Team")
                feedback_df = gr.Dataframe(interactive=False, height=300)
                load_feedback_btn = gr.Button("📋 Load Feedback", variant="secondary")
                load_feedback_btn.click(get_recent_feedback, outputs=feedback_df)
        
        gr.Markdown("---")
        
        with gr.Row():
            with gr.Column():
                gr.Markdown("#### ✍️ Submit Model Feedback")
                gr.Markdown("*Tell us if the AI got it right or wrong*")
                fb_model = gr.Dropdown(
                    ["churn_prediction", "demand_forecast", "product_recommendation", "next_purchase"], 
                    label="Model Name"
                )
                fb_entity = gr.Textbox(label="Entity ID (Customer/Product ID)", placeholder="e.g., 12345")
                fb_pred_value = gr.Number(label="Prediction Value (optional)", precision=2)
                fb_actual = gr.Textbox(label="Actual Outcome (optional)", placeholder="What actually happened?")
                fb_type = gr.Dropdown(
                    ["correct_prediction", "wrong_prediction", "override_needed", "edge_case"], 
                    label="Feedback Type"
                )
                fb_notes = gr.Textbox(label="Notes", lines=2, placeholder="Add context about why the prediction was right/wrong")
                fb_submit = gr.Button("✅ Submit Feedback", variant="primary")
                fb_result = gr.Textbox(label="Result", lines=4)
                fb_submit.click(
                    submit_model_feedback,
                    inputs=[fb_model, fb_entity, fb_pred_value, fb_actual, fb_type, fb_notes],
                    outputs=fb_result
                )
            
            with gr.Column():
                gr.Markdown("#### 🔄 Override AI Prediction")
                gr.Markdown("*Use your business knowledge to override the AI*")
                ov_customer_id = gr.Number(label="Customer ID", precision=0, info="Which customer?")
                ov_original = gr.Number(label="Original AI Probability (optional)", minimum=0, maximum=1, precision=3)
                ov_override = gr.Number(label="Your Override Probability", minimum=0, maximum=1, precision=3, info="0.0 = will not churn, 1.0 = will definitely churn")
                ov_reason = gr.Textbox(label="Reason", placeholder="e.g., Just renewed contract, Strategic partnership")
                ov_valid_days = gr.Number(label="Valid for (days, leave blank = permanent)", precision=0, info="After X days, AI takes over again")
                ov_submit = gr.Button("🔄 Submit Override", variant="primary")
                ov_result = gr.Textbox(label="Result", lines=4)
                ov_submit.click(
                    override_churn_prediction,
                    inputs=[ov_customer_id, ov_original, ov_override, ov_reason, ov_valid_days],
                    outputs=ov_result
                )
    
    # TAB 2: Inventory Adjustments
    with gr.Tab("📦 Inventory Adjustments"):
        gr.Markdown("### Reconcile Physical Counts with System Records")
        
        with gr.Row():
            with gr.Column():
                gr.Markdown("#### Current Inventory Levels")
                inv_df = gr.Dataframe(interactive=False, height=350)
                load_inv_btn = gr.Button("📊 Load Inventory", variant="primary")
                load_inv_btn.click(get_inventory_status, outputs=inv_df)
            
            with gr.Column():
                gr.Markdown("#### Recent Adjustments by Team")
                adj_df = gr.Dataframe(interactive=False, height=350)
                load_adj_btn = gr.Button("📋 Load Adjustments", variant="secondary")
                load_adj_btn.click(get_recent_adjustments, outputs=adj_df)
        
        gr.Markdown("---")
        gr.Markdown("#### ✍️ Submit Physical Count Adjustment")
        gr.Markdown("*When warehouse count doesn't match the system*")
        
        with gr.Row():
            inv_product_id = gr.Number(label="Product ID", precision=0, info="Which product?")
            inv_warehouse_id = gr.Number(label="Warehouse ID", precision=0, info="Which warehouse?")
            inv_old_qty = gr.Number(label="System Quantity", precision=0, info="What the system shows")
            inv_new_qty = gr.Number(label="Physical Count", precision=0, info="What you actually counted")
        
        with gr.Row():
            inv_reason = gr.Dropdown(
                ["shrinkage", "damage", "theft", "miscount", "cycle_count"], 
                label="Reason Code",
                info="Why is there a difference?"
            )
            inv_location = gr.Textbox(label="Location (optional)", placeholder="Aisle 3, Bin 12")
            inv_notes = gr.Textbox(label="Notes (optional)", lines=2, placeholder="Additional details")
        
        inv_submit = gr.Button("✅ Submit Adjustment", variant="primary")
        inv_result = gr.Textbox(label="Result", lines=4)
        inv_submit.click(
            submit_inventory_adjustment,
            inputs=[inv_product_id, inv_warehouse_id, inv_old_qty, inv_new_qty, inv_reason, inv_location, inv_notes],
            outputs=inv_result
        )
    
    # TAB 3: Customer Priority
    with gr.Tab("Customer Priority"):
        gr.Markdown("### Flag Strategic Customers for Special Treatment")
        
        with gr.Row():
            with gr.Column():
                gr.Markdown("#### Customer List (Top by Revenue)")
                cust_df = gr.Dataframe(interactive=False, height=350)
                load_cust_btn = gr.Button("📊 Load Customers", variant="primary")
                load_cust_btn.click(get_customers, outputs=cust_df)
            
            with gr.Column():
                gr.Markdown("#### Active Priority Flags")
                flag_df = gr.Dataframe(interactive=False, height=350)
                load_flag_btn = gr.Button("📋 Load Flags", variant="secondary")
                load_flag_btn.click(get_priority_flags, outputs=flag_df)
        
        gr.Markdown("---")
        gr.Markdown("#### ✍️ Set Customer Priority")
        gr.Markdown("*Mark VIP, Strategic, or At-Risk customers*")
        
        with gr.Row():
            cp_customer_id = gr.Number(label="Customer ID", precision=0, info="Which customer?")
            cp_priority = gr.Dropdown(
                ["VIP", "Strategic", "At-Risk", "White-Glove", "Standard"], 
                label="Priority Level",
                info="How important is this customer?"
            )
            cp_flag_type = gr.Dropdown(
                ["strategic_account", "high_value", "retention_focus", "support_priority"], 
                label="Flag Type",
                info="What kind of special treatment?"
            )
        
        with gr.Row():
            cp_reason = gr.Textbox(label="Reason", placeholder="e.g., Top revenue account, Partnership opportunity, Risk of leaving")
            cp_valid_days = gr.Number(label="Valid for (days, leave blank = permanent)", precision=0, info="After X days, flag expires")
            cp_notes = gr.Textbox(label="Notes (optional)", lines=2, placeholder="Additional context")
        
        cp_submit = gr.Button("✅ Set Priority", variant="primary")
        cp_result = gr.Textbox(label="Result", lines=4)
        cp_submit.click(
            set_customer_priority,
            inputs=[cp_customer_id, cp_priority, cp_flag_type, cp_reason, cp_valid_days, cp_notes],
            outputs=cp_result
        )
    
    # TAB 4: Data Quality
    with gr.Tab("🔍 Data Quality Issues"):
        gr.Markdown("### Report Data Problems You Spot")
        
        dq_issues_df = gr.Dataframe(interactive=False, height=400)
        load_dq_btn = gr.Button("📋 Load All Issues", variant="primary")
        load_dq_btn.click(get_data_quality_issues, outputs=dq_issues_df)
        
        gr.Markdown("---")
        gr.Markdown("#### ✍️ Report Data Quality Issue")
        gr.Markdown("*Help keep our data clean - report what's wrong*")
        
        with gr.Row():
            dq_table = gr.Textbox(
                label="Table Name", 
                placeholder="sap_dev.gold.churn_predictions",
                info="Which table has the problem?"
            )
            dq_column = gr.Textbox(
                label="Column Name (optional)", 
                placeholder="ChurnProbability",
                info="Which column? Leave blank if table-level issue"
            )
            dq_issue_type = gr.Dropdown(
                ["missing_data", "incorrect_value", "duplicate", "formatting", "logic_error"], 
                label="Issue Type",
                info="What kind of problem?"
            )
            dq_severity = gr.Dropdown(
                ["critical", "high", "medium", "low"], 
                label="Severity",
                info="How serious is it?"
            )
        
        dq_description = gr.Textbox(
            label="Description", 
            lines=3, 
            placeholder="Describe the issue: What's wrong? When did you notice it? What should it be?"
        )
        dq_samples = gr.Textbox(
            label="Sample Records (optional)", 
            lines=2, 
            placeholder="CustomerID=123 has probability=2.5 (should be 0-1), OrderID=456 is duplicated"
        )
        
        dq_submit = gr.Button("✅ Report Issue", variant="primary")
        dq_result = gr.Textbox(label="Result", lines=4)
        dq_submit.click(
            report_data_quality_issue,
            inputs=[dq_table, dq_column, dq_issue_type, dq_severity, dq_description, dq_samples],
            outputs=dq_result
        )
    
    # TAB 5: Demand Forecast
    with gr.Tab("Demand Forecast Adjustment"):
        gr.Markdown("### Adjust AI Forecasts with Your Business Knowledge")
        
        with gr.Row():
            with gr.Column():
                gr.Markdown("#### AI Demand Forecasts")
                df_df = gr.Dataframe(interactive=False, height=350)
                load_df_btn = gr.Button("📊 Load Forecasts", variant="primary")
                load_df_btn.click(get_demand_forecasts, outputs=df_df)
            
            with gr.Column():
                gr.Markdown("#### Recent Adjustments by Team")
                dfa_df = gr.Dataframe(interactive=False, height=350)
                load_dfa_btn = gr.Button("📋 Load Adjustments", variant="secondary")
                load_dfa_btn.click(get_forecast_adjustments, outputs=dfa_df)
        
        gr.Markdown("---")
        gr.Markdown("#### ✍️ Adjust Demand Forecast")
        gr.Markdown("*Factor in promotions, market events, competitive actions the AI doesn't know about*")
        
        with gr.Row():
            dfa_product_id = gr.Number(label="Product ID", precision=0, info="Which product?")
            dfa_date = gr.Textbox(label="Forecast Date", placeholder="2026-09-15", info="Format: YYYY-MM-DD")
            dfa_ml_qty = gr.Number(label="AI Forecast Qty", precision=0, info="What the AI predicts")
            dfa_factor = gr.Number(
                label="Adjustment Factor", 
                value=1.0, 
                precision=2,
                info="1.0=no change, 1.5=+50% increase, 0.8=-20% decrease"
            )
        
        with gr.Row():
            dfa_reason = gr.Dropdown(
                ["promotion", "market_event", "competitor_action", "supply_constraint"], 
                label="Reason",
                info="Why are you adjusting?"
            )
            dfa_event = gr.Textbox(
                label="Event Type (optional)", 
                placeholder="Black Friday, Product Launch, Seasonal Sale"
            )
            dfa_notes = gr.Textbox(label="Notes (optional)", lines=2, placeholder="Additional details about the adjustment")
        
        dfa_submit = gr.Button("✅ Submit Adjustment", variant="primary")
        dfa_result = gr.Textbox(label="Result", lines=4)
        dfa_submit.click(
            adjust_demand_forecast,
            inputs=[dfa_product_id, dfa_date, dfa_ml_qty, dfa_factor, dfa_reason, dfa_event, dfa_notes],
            outputs=dfa_result
        )
    
    # Load initial data on startup
    demo.load(get_churn_predictions, outputs=churn_df)

# Mount Gradio on FastAPI (required for Databricks Apps proxy)
from fastapi import FastAPI
import uvicorn

fastapi_app = FastAPI()
app = gr.mount_gradio_app(fastapi_app, demo, path="/")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
