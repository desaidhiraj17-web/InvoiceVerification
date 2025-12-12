from fastapi import APIRouter, Depends, HTTPException, status, Request, File, UploadFile, Form, Query
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from src.db.database import get_db
#  *****************   Models Import  *******************
from src.models.auth import User
from src.models.invoices import PriorityLevel, InvoiceStatus

#  *****************   Services Import  *******************
from src.services.invoices import read_csv_file, invoices_apply_filters_search_pagination, prepare_invoice_upload_data, \
        save_invoice_upload_data, paginate_query, get_invoice_details, prepare_party_master_data, save_party_master_data, \
        prepare_product_master_data, save_product_master_data, check_rack_no_rack_master, prepare_rack_master_data, \
        save_rack_master_data, delete_invoice_product, add_invoice_product, preparing_fields_invoice_metadata, \
        insert_into_invoice_metadata, add_transactions, check_tray_no_tray_master, prepare_tray_master_data, save_tray_master_data, \
        get_user_productivity_report, compute_performance_metrics, detect_operation_status
from src.services.user_services import get_current_user

#  *****************  Helpers Import  *******************
from src.helpers.invoices import FileUploadType, FlowType,list_invoices_base_query, invoices_return_structure, list_invoices_products_base_query, \
            check_invoice_exists, check_duplicate_csv_product_master,update_invoice_status, epoch_to_str,check_invoice_metadata_fields_exist, \
            invoices_metadata_field_map , invoice_metadata_row_exists, check_invoice_product_exists
            
#  *****************  Schemas Import  *******************
from src.schemas.invoices import InvoiceMetadataUpdateSchema,InvoiceProductActionSchema, TransactionAdd, PerformanceDashboardFilter


from sqlalchemy.future import select
from sqlalchemy import or_, text
import uuid
from typing import Dict, Optional
from src.logger.logger_setup import logger
from datetime import datetime

router = APIRouter(tags=["Invoices"])


@router.post("/file_upload")
async def file_upload(db: AsyncSession = Depends(get_db),
                    file:UploadFile = File(...), 
                    value: FileUploadType = Form(...),
                    current_user: User = Depends(get_current_user)):

    """ Uploads and processes CSV files for Invoice, Party Master, Product Master, Rack Master, and Tray Master.
        Product Master file must be uploaded before Invoice upload to ensure proper rack mapping.
        Validates, prepares, and stores data based on the selected upload type."""

    try:
        logger.info(f"File upload api started : {value}")
        rows = await read_csv_file(file)
        if value == FileUploadType.invoice: 
            party_rows, invoice_rows, product_rows = await prepare_invoice_upload_data(db,rows,current_user)
            
            await save_invoice_upload_data(db,party_rows, invoice_rows, product_rows)
            logger.info("file upload invoice api run successfully")
            return {
            "status" : "success",
            "message" : "Invoices data added successfully",
            }
        
        if value == FileUploadType.party_master:
            prepared_data_party_master = await prepare_party_master_data(db,rows,current_user)
            await save_party_master_data(db,prepared_data_party_master)
            logger.info("file upload party master api run successfully")
            return {
            "status" : "success",
            "message" : "Party Master data added successfully",
            }
        
        if value == FileUploadType.product_master:
            # await check_duplicate_csv_product_master(rows)
            records = await prepare_product_master_data(rows,current_user)
            message = await save_product_master_data(db,records)
            return {
            "status" : "success",
            **message
            }
        
        if value == FileUploadType.rack_master:
            await check_rack_no_rack_master(rows)
            # await check_duplicate_csv_rack_master(rows)
            prepared_rack_master = await prepare_rack_master_data(db, rows, current_user)
            message = await save_rack_master_data(db, prepared_rack_master)
            logger.info("file upload rack master api run successfully")
            return {
                "status": "success",
                **message
            }
            
        if value == FileUploadType.tray_master:
            # await validate_tray_master_csv(db, rows)
            await check_tray_no_tray_master(db,rows)
            prepared = await prepare_tray_master_data(rows)
            message = await save_tray_master_data(db, prepared)

            logger.info("file upload tray master api run successfully")
            return {
                "status": "success",
                **message
            }
                
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error uploading file: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})
    
    

@router.get("/")
async def invoices( type: FlowType,db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
    search: str | None = Query(None, description="Search by invoice number or party code or party name"),
    priority: Optional[PriorityLevel] = Query(None, description="Filter by priority level (HIGH, MEDIUM, LOW)"),
    
    from_date: str | None = Query(None, description="Format: DD-MM-YYYY"),
    to_date: str | None = Query(None, description="Format: DD-MM-YYYY"),
    is_verified: bool | None = Query(None, description="true = verified only, false = unverified only, none = unverified first then verified"),

    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100)
    ):
    """Fetches paginated invoices for the authenticated user with search and filter support.
    Allows filtering by priority, verification status, and date range, along with flexible text search.
    Allows searching by invoice number or party code or party name
    Returns total count, pagination info, and structured invoice data."""
    
    try:
        logger.info("Invoices get api started")
        base_query = list_invoices_base_query()
        
        rows,total = await invoices_apply_filters_search_pagination(type,db,base_query,search,priority,from_date,to_date,is_verified,page,page_size)
        
        if not rows:
            logger.error("Invoices data not found")
            return {
                "status" : "success",
                "message" : "Data fetched successfully",
                "data":{"invoices": []}
                }  # return empty list if no invoices found

        invoice_map=invoices_return_structure(rows)

        data= {
            "page": page,
            "page_size": page_size,
            "total": total,
            "invoices": list(invoice_map.values())
        }
        logger.info("Invoices get api runs successfully")
        return {
            "status" : "success",
            "message" : "Data fetched successfully",
            "data":data
        }
    except Exception as e:
        logger.exception(f"Invoices get api: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})
    

@router.get("/{invoice_id}/products")
async def invoices_products(invoice_id:str, 
                type: FlowType,
                rack_no: str | None = Query(None, description="Filter by rack_no"),
                db: AsyncSession = Depends(get_db),
                current_user: User = Depends(get_current_user),
                page: int = Query(1, ge=1),
                page_size: int = Query(10, ge=1, le=100)):
    
    """Retrieves paginated product line items for a specific invoice.
    Supports optional filtering by rack number and applies alphabetical and priority-based sorting.
    Returns invoice details along with total count and paginated product data."""
    
    try:
        logger.info("invoices_products api started")
        exists = await check_invoice_exists(db, invoice_id)
        if not exists:
                logger.error("Invoice not found")
                raise HTTPException(status_code=404, detail={"status" : "error",
                "message" : "Invoice Not found"})
        
        invoice_details = await get_invoice_details(db, invoice_id)
        
        base_query = list_invoices_products_base_query(type,rack_no=rack_no)
        
        order_by = """
            ORDER BY 
                LOWER(ip.product_name) ASC,
                CASE inv.priority
                    WHEN 'HIGH' THEN 1
                    WHEN 'MEDIUM' THEN 2
                    WHEN 'LOW' THEN 3
                END ASC
        """
        params = {"invoice_id": invoice_id}
        if rack_no:
            params["rack_no"] = rack_no

        pagination_result = await paginate_query(db=db,base_query=base_query,params=params,page=page,
            page_size=page_size,order_by=order_by,
        )
        logger.info("Invoices Products fetched successfully")
        
        response_data = {
            "total": pagination_result["total"],
            "page": pagination_result["page"],
            "page_size": pagination_result["page_size"],
            "invoice": dict(invoice_details),
            "lines":pagination_result["data"]
        }
        return {
        "status" : "success",
        "message" : "Invoices Products fetched successfully",
        "data": response_data
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Inside invoices_products api: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100], "data":[]})
        
    

@router.put("/{invoice_id}/priority") 
async def invoice_priority(invoice_id:str,priority:PriorityLevel,db: AsyncSession = Depends(get_db),
                current_user: User = Depends(get_current_user)) :
    """
        Updates the priority level of a specific invoice.
        Validates invoice existence and maps priority enum (HIGH, MEDIUM, LOW) before updating.
        Returns success message after updating the invoice priority.
    """
    
    try:
        invoice_check_query = "Select * from invoices where id = :invoice_id;"
        result = await db.execute(text(invoice_check_query),{"invoice_id":invoice_id})
        invoice = result.mappings().first()
        
        if not invoice:
            logger.error(f"Invoice {invoice_id} not found")
            raise HTTPException(
                status_code=404,
                detail={"status":"error","message": f"Invoice {invoice_id} not found"}
            )
        PRIORITY_MAP = {
            "1": "HIGH",
            "2": "MEDIUM",
            "3": "LOW"
        }
        priority_update_query = "update invoices set priority = :priority, updated_at = :updated_at where id = :invoice_id; "
        params={
            "priority":PRIORITY_MAP.get(priority), 
            "updated_at":datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            "invoice_id":invoice_id
            }
        await db.execute(text(priority_update_query),params)
        await db.commit()
        logger.info(f"Invoice {invoice_id} priority updated to {priority}")
        return {"status":"success","message":f"Invoice {invoice_id} priority updated to {priority}"}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Inside invoices_products api: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100], "data":[]})
        

@router.put("/{invoice_id}/invoice_metadata")
async def invoice_metadata_update_view(invoice_id:str, data: InvoiceMetadataUpdateSchema, 
                db: AsyncSession = Depends(get_db),
                current_user: User = Depends(get_current_user)):
    
    """ Updates invoice metadata timestamps (picker, checker, packer) and invoice status.
        Validates invoice existence and updates only valid, non-duplicate *_start and *_end fields.
        Updates the invoice status based on the provided metadata.
        Triggers performance metrics computation when the invoice is marked as checking_end, picking_end, or completed
        (i.e., when the Mark as Complete action is performed).
    """
    
    try:
        now_str = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        exists = await check_invoice_exists(db, invoice_id)
        if not exists:
            logger.error(f"Invoice: {invoice_id} not found")
            raise HTTPException(status_code=404, detail={"status" : "error",
            "message" : f"Invoice: {invoice_id} Not found"})
            
        input_fields = await check_invoice_metadata_fields_exist(data)
            
        existing_row = await invoice_metadata_row_exists(db,invoice_id)
        
        update_fields, status_value = await preparing_fields_invoice_metadata(data,existing_row,input_fields,invoice_id,current_user)
        if not update_fields:
            logger.info(f"No metadata fields updated for invoice {invoice_id} — *_start field already set.")
            return {
                "status": "success",
                "message": f"No new metadata fields updated for invoice {invoice_id} (start fields already exist).",
            }
        
        await insert_into_invoice_metadata(db,existing_row,update_fields,invoice_id)
            
        await update_invoice_status(db,invoice_id,status_value,now_str)

        await db.commit()
        logger.info(f"Invoice status and metadata added successfully")
        
        if status_value in [
            InvoiceStatus.checking_end,
            InvoiceStatus.picking_end,
            InvoiceStatus.completed
        ]:
            try:
                operation_status = detect_operation_status(data)
                await compute_performance_metrics(db, invoice_id,data,operation_status=operation_status)
                logger.info(f"Performance metrics computed for invoice {invoice_id}")
            except Exception as e:
                logger.exception(f"Performance metrics failed for invoice {invoice_id}: {e}")
                #  Do NOT rollback main transaction — only log failure
        

        return {
            "status":"success",
            "message":f"Invoice status and metadata added successfully",
            "data": update_fields
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Inside invoices_metadata_update_view api: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100], "data":[]})
        
        

@router.post("/{invoice_id}/product")
async def invoice_product_add_delete(invoice_id:str, type: FlowType,
                data: InvoiceProductActionSchema, db: AsyncSession = Depends(get_db),
                current_user: User = Depends(get_current_user)):
    """ 
    Adds or deletes a product from a specific invoice based on the requested action.
    Validates invoice existence and supports dynamic product creation with batch, expiry, quantity, rack, and scan status.
    Formats expiry date and assigns rack number during product addition.
    Supports safe deletion with proper validation and error handling.
    """
    try:
        if data.action not in ["add", "delete"]:
            logger.exception("Invalid value, must be 'add' or 'delete'")
            raise HTTPException(
                status_code=400,
                detail={"status": "error", "message": "Invalid value, must be 'add' or 'delete'"}
            )
            
        exists = await check_invoice_exists(db, invoice_id)
        if not exists:
            logger.error(f"Invoice not found for invoice_id: {invoice_id}")
            raise HTTPException(status_code=404, detail={"status" : "error",
            "message" : f"Invoice not found for invoice_id: {invoice_id}"})
            
            
        if data.action == "delete":
            await delete_invoice_product(db,data.product_id)
            logger.info(f"Productid: {data.product_id} deleted from invoice_product_list")
            return {"status": "success", "message": "Product deleted successfully"}
        
        elif data.action == "add":
            # Check if already exists
            created = await add_invoice_product(db,invoice_id,data,type)
            logger.info(f"Product added successfully to invoice: {invoice_id}")
            return {"status": "success", "message": f"Product added successfully to invoice: {invoice_id}",
                "product":created}

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Inside invoices_metadata_update_view api: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100], "data":[]})
    
    
@router.delete("/{invoice_id}")
async def invoice_delete(invoice_id:str, db: AsyncSession = Depends(get_db),
            current_user: User = Depends(get_current_user)):
    """ Deletes an invoice along with its related invoice products and metadata.
        Validates invoice existence before deletion and ensures transactional safety."""
    try:
        exists = await check_invoice_exists(db, invoice_id)
        if not exists:
            logger.error(f"Invoice not found for invoice_id: {invoice_id}")
            raise HTTPException(status_code=404, detail={"status" : "error",
            "message" : f"Invoice not found for invoice_id: {invoice_id}"})

        await db.execute(
            text("""DELETE FROM invoice_product_list WHERE invoice_id = :invoice_id"""),
            {"invoice_id": invoice_id}
        )
        
        await db.execute(
            text("""DELETE FROM invoice_metadata WHERE invoice_id = :invoice_id"""),
            {"invoice_id": invoice_id}
        )
        
        result = await db.execute(
            text("""DELETE FROM invoices WHERE id = :invoice_id"""),
            {"invoice_id": invoice_id}
        )

        if result.rowcount == 0:
            await db.rollback()
            logger.exception(f"Invoice : {invoice_id} deletion failed")
            raise HTTPException(status_code=400, detail={"status": "error", "message": f"Invoice : {invoice_id} deletion failed"})
        
        await db.commit()
        logger.info(f"Invoice & products deleted successfully for invoice: {invoice_id}")
        return {"status": "success", "message": "Invoice & products deleted successfully"}
            
    except HTTPException:
            raise
    except Exception as e:
        await db.rollback()
        logger.exception(f"Inside invoice_delete_view api: {e}")
        raise HTTPException(status_code=500, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100], "data":[]})


    
@router.post("/transactions/add")
async def transactions_add(data: TransactionAdd, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    
    """ Records scan transactions for one or more products under a specific invoice.
        Validates invoice and product existence before bulk inserting transaction records.
        Stores operation type, status, scan status, image, and rack information per transaction."""
    
    try:
        invoice_exists = await check_invoice_exists(db, data.invoice_id)
        if not invoice_exists:
            logger.error(f"Invoice: {data.invoice_id} not found")
            raise HTTPException(status_code=404, detail={"status" : "error",
            "message" : f"Invoice: {data.invoice_id} Not found"})
        
        await check_invoice_product_exists(data,db)
        
        await add_transactions(data,db,current_user)
        
        return {
            "status": "success",
            "message": "Transaction inserted successfully"
        }
    except HTTPException:
            raise
    except Exception as e:
        logger.exception(f"inside add_transaction: {str(e)}")
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail={"status": "error", "message": str(e)}
        )
        
        
# @router.get("/transactions")
# async def transactions_dashboard(db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
#     try:
#         # query = text("""
#         # SELECT user_id,invoice_id, MIN(timestamp) AS start_time,
#         #     (
#         #         SELECT MAX(t2.timestamp)
#         #         FROM transactions t2
#         #         WHERE t2.invoice_id = t1.invoice_id
#         #         AND t2.scan_status = 'success'
#         #     ) AS complete_time,
            
#         #     (
#         #         SELECT COUNT(*)
#         #         FROM transactions t3
#         #         WHERE t3.invoice_id = t1.invoice_id
#         #     ) AS total_entries,
            
#         #     CAST(
#         #         (
#         #             strftime('%s',
#         #             (
#         #                 SELECT MAX(
#         #                 substr(t2.timestamp, 7, 4) || '-' || substr(t2.timestamp, 4, 2) || '-' || substr(t2.timestamp, 1, 2)
#         #                 || substr(t2.timestamp, 11)
#         #             )
#         #                 FROM transactions t2
#         #                 WHERE t2.invoice_id = t1.invoice_id
#         #                 AND t2.scan_status = 'success'
#         #                 )
#         #             )
#         #             -
#         #             strftime('%s',
#         #             substr(MIN(t1.timestamp), 7, 4) || '-' || substr(MIN(t1.timestamp), 4, 2) || '-' || substr(MIN(t1.timestamp), 1, 2)
#         #             || substr(MIN(t1.timestamp), 11)
#         #             )
#         #         ) AS INTEGER
#         #     ) AS duration_seconds,
            
#         #     ROUND(
#         #             (
#         #                 (SELECT COUNT(*) FROM transactions t3 WHERE t3.invoice_id=t1.invoice_id) 
#         #                 * 60.0
#         #             )
#         #             /
#         #             (
#         #                 CAST(
#         #                     (
#         #                         strftime('%s',
#         #                             (SELECT MAX(
#         #                                 substr(t2.timestamp,7,4)||'-'||substr(t2.timestamp,4,2)||'-'||substr(t2.timestamp,1,2)||substr(t2.timestamp,11)
#         #                             ) FROM transactions t2 
#         #                             WHERE t2.invoice_id=t1.invoice_id AND t2.scan_status='success'
#         #                             )
#         #                         )
#         #                         -
#         #                         strftime('%s',
#         #                             substr(MIN(t1.timestamp),7,4)||'-'||substr(MIN(t1.timestamp),4,2)||'-'||substr(MIN(t1.timestamp),1,2)||substr(MIN(t1.timestamp),11)
#         #                         )
#         #                     ) AS INTEGER
#         #                 )
#         #             )
#         #         ,3) AS entries_per_minute
#         # FROM transactions t1
#         # GROUP BY invoice_id
#         # """)
       
#         base_query = get_user_productivity_report()
#         result = await db.execute(text(base_query))
#         return {"status": "success", "data": result.mappings().all()}
#     except HTTPException:
#             raise
#     except Exception as e:
#         logger.exception(f"inside transactions_dashboard: {str(e)}")
#         raise HTTPException(
#             status_code=500,
#             detail={"status": "error", "message": str(e)}
#         )
        
        
@router.post("/performance_dashboard")
async def get_performance_dashboard(data: PerformanceDashboardFilter, db: AsyncSession = Depends(get_db), 
        current_user: User = Depends(get_current_user)):
    try:
        pass
    except HTTPException:
            raise
    except Exception as e:
        logger.exception(f"inside get_performance_dashboard: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={"status": "error", "message": str(e)}
        )
    
        

