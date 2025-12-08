from fastapi import APIRouter, Depends, HTTPException, status, Request, File, UploadFile, Form, Query
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from src.db.database import get_db
from sqlalchemy.future import select
from sqlalchemy import or_, text
import uuid
from typing import Dict, Optional
from src.logger.logger_setup import logger
from src.services.user_services import get_current_user
import asyncio

#  *****************   Models Import  *******************
from src.models.auth import User

#  ***************** services  Import  *******************
from src.services.products import match_scan,scan_quantity_update_products,release_trays_if_completed, get_product_qty_converter_count, \
    get_product_qty_converter_data, product_qty_converter_exist, update_product_qty_converter_values
from src.services.invoices import search_batch_number_invoice

#  ***************** Helpers Import  *******************
from src.helpers.invoices import check_invoice_exists

#  *****************   Schemas Import  *******************
from src.schemas.products import MatchScanRequest, UpdateTrayInvoiceRequest, ProductScanUpdate,ProductScanQtyUpdate, \
    ProductQtyConverterListResponse, UpdateProductQtyConverterSchema


router = APIRouter(tags=["Products"])


@router.post("/match/scan")
async def match_scan_product(
                data: MatchScanRequest,
                db: AsyncSession = Depends(get_db),
                current_user: User = Depends(get_current_user),
                ):
    try:
        exists = await check_invoice_exists(db, data.invoice_id)
        if not exists:
                logger.error("Invoice not found")
                raise HTTPException(status_code=404, detail={"status" : "error",
                "message" : "Invoice Not found"})
        
        if (not data.batch_number) or (not data.expiry_date) or (not data.mrp):
            logger.error(" to search products")
            raise HTTPException(status_code=400, detail={"status":"error", "message":"Not sufficient parameters to search products",
                                                        "data":[]})
        
        result = await match_scan(db,data)
        return result


    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100], "data":[]})
        
        
@router.get("/search/batch_no")
async def get_products_batch_number(
            batch_number: str = Query(..., min_length=3, description="Enter at least 3 characters of batch number"),
            invoice_id: str = Query(None, description="Invoice ID for fallback search"),
            page: int = Query(1, ge=1, description="Page number (1-indexed)"),
            page_size: int = Query(10, ge=1, le=50, description="Number of records per page"),
            db: AsyncSession = Depends(get_db),
            current_user: User = Depends(get_current_user)):
    try:
        where_clause = "LOWER(batch_number) LIKE LOWER(:batch_number_pattern)"

        # Pagination offset and limit
        offset = (page - 1) * page_size
        count_query = f"SELECT COUNT(*) FROM product_master WHERE {where_clause}"
        count_result = await db.execute(text(count_query), {"batch_number_pattern": f"{batch_number}%"})
        total = count_result.scalar() or 0
        
        if total > 0:
            # logger.info("No products found for given batch number")
            # return {
            #     "status": "success",
            #     "message": "No products found for the given batch number",
            #     "page": page,
            #     "page_size": page_size,
            #     "total": 0,
            #     "data": []
            # }
            base_query = f"""
                    SELECT * FROM product_master
                    WHERE {where_clause}
                    ORDER BY 
                        substr(updated_at, 7, 4) || '-' || substr(updated_at, 4, 2) || '-' || substr(updated_at, 1, 2) || ' ' || substr(updated_at, 12)
                    DESC
                    LIMIT :limit OFFSET :offset
                    """
            params = {
                "batch_number_pattern": f"{batch_number}%",
                "limit":page_size,
                "offset":offset
            }
            result = await db.execute(text(base_query),params)
            rows = result.mappings().all()
            logger.info(f"Found {len(rows)} products for batch pattern '{batch_number}' in ProductMaster")
            return {
                "status": "success",
                "message": f"Found {len(rows)} products in ProductMaster",
                "page": page,
                "page_size": page_size,
                "total": total,
                "data": rows
            }
        if not invoice_id:
            logger.info("No products found and no invoice_id provided")
            return {
                "status": "success",
                "message": "No products found and no invoice_id provided",
                "page": page,
                "page_size": page_size,
                "total": 0,
                "data": []
            }
        
        products = await search_batch_number_invoice(db,invoice_id,batch_number,page,page_size)
        return products
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error searching batch_number: {e}")
        raise HTTPException(
        status_code=500, detail={
            "status": "error",
            "message": str(e).split("\n")[0][:100],
            "data": []
            }
        )
        
        
@router.get("/rack")
async def get_racks(
            page: int = Query(1, ge=1, description="Page number (1-indexed)"),
            page_size: int = Query(10, ge=1, le=50, description="Number of records per page"),
            db: AsyncSession = Depends(get_db),
            current_user: User = Depends(get_current_user)):
    try:
        
        # Pagination offset and limit
        offset = (page - 1) * page_size
        count_query = f"SELECT COUNT(*) FROM rack_master;"
        count_result = await db.execute(text(count_query))
        total = count_result.scalar() or 0
        
        if total == 0:
            logger.info("Racks not found.")
            return {
                "status": "success",
                "message": "Racks not found",
                "page": page,
                "page_size": page_size,
                "total": 0,
                "data": []
            }
        base_query = f"""
                SELECT * FROM rack_master
                ORDER BY 
                    substr(updated_at, 7, 4) || '-' || substr(updated_at, 4, 2) || '-' || substr(updated_at, 1, 2) || ' ' || substr(updated_at, 12)
                DESC
                LIMIT :limit OFFSET :offset
                """
        result = await db.execute(text(base_query), {"limit":page_size,"offset":offset})
        rows = result.mappings().all()
        logger.info(f"{len(rows)} racks found.")
        return {
            "status": "success",
            "message": f"{len(rows)} racks found.",
            "page": page,
            "page_size": page_size,
            "total": total,
            "data": rows
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error in get_rack api: {e}")
        raise HTTPException(
        status_code=500, detail={
            "status": "error",
            "message": str(e).split("\n")[0][:100],
            "data": []
            }
        )
        
        
@router.get("/qty-converter", response_model=ProductQtyConverterListResponse)
async def get_product_qty_converter_list(
    db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user),
    page: int = 1,
    page_size: int = 10,
    search: Optional[str] = None
):
    try:
        total = await get_product_qty_converter_count(db, search)

        records = await get_product_qty_converter_data(
            db=db,
            page=page,
            page_size=page_size,
            search=search
        )

        logger.info("Get Method of /qty-converter api executed successfully")
        return {
            "status": "success",
            "total": total,
            "page": page,
            "page_size": page_size,
            "data": records
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(str(e))
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": str(e).split("\n")[0][:200]
            }
        )
        
        
@router.put("/qty-converter")  
async def update_product_qty_converter(
    data: UpdateProductQtyConverterSchema,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    try:
        # Check if product exists
        await product_qty_converter_exist(db,data)
        update_fields,params = await update_product_qty_converter_values(data,current_user)

        update_query = f"""
            UPDATE product_qty_converter
            SET {", ".join(update_fields)},
                updated_at = :updated_at,
                updated_by = :updated_by
            WHERE product_name = :product_name
            RETURNING 
                id,
                product_name,
                item_code,
                shipper_val,
                box_val,
                strip_val,
                updated_at,
                updated_by;
        """

        result = await db.execute(text(update_query), params)
        updated_product = result.mappings().first()

        if not updated_product:
            logger.error(f"Product {data.product_name} not found")
            raise HTTPException(
                status_code=404,
                detail={"status": "error", "message": f"Product {data.product_name} not found"}
            )
        await db.commit()
        logger.info(f"Product {data.product_name} quantity updated successfully")
        return {
            "status": "success",
            "message": f"Product {data.product_name} quantity updated successfully",
            "data": updated_product
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(str(e))
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail={"status": "error", "message": str(e).split("\n")[0][:100]}
        ) 
        
    
@router.get("/{tray_no}")
async def get_invoice_no(
            tray_no: str ,
            db: AsyncSession = Depends(get_db),
            current_user: User = Depends(get_current_user)):
    try:    
        query = text("""
            SELECT tm.id, tm.tray_no, tm.tray_qr_value, tm.current_invoice_no AS invoice_id
            FROM tray_master tm
            WHERE tm.tray_no = :tray_no
        """)
        
        result = await db.execute(query, {"tray_no": tray_no})
        tray = result.mappings().first()

        if not tray:
            logger.exception(f"No tray found for tray_no '{tray_no}'")
            raise HTTPException(
                status_code=404,
                detail={"status": "error", "message": f"No tray found for tray_no: '{tray_no}'"}
            )
            
        if tray["invoice_id"] is None:
            logger.exception(f"Invoice ID not found for tray: '{tray_no}'")
            raise HTTPException(
                status_code=404,
                detail={"status": "error", "message": f"Invoice ID not found for tray: '{tray_no}'"}
            )
        logger.info(f"Invoice_id found successfully for tray_no: {tray_no}")
        return {
            "status": "success",
            "message":f"Invoice_id found successfully for tray_no {tray_no}",
            "data": tray
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error in get_invoice_no for tray api: {e}")
        raise HTTPException(
        status_code=500, detail={
            "status": "error",
            "message": str(e).split("\n")[0][:100],
            }
        )


@router.put("/tray/{tray_no}/invoice", summary="Update current invoice for a specific tray")
async def update_tray_invoice(
    tray_no: str,
    request: UpdateTrayInvoiceRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
    ):
    """
    Update or remove the current invoice associated with a specific tray_no.
    """
    try:
        #  Check if tray exists
        check_query = text("SELECT id FROM tray_master WHERE tray_no = :tray_no")
        tray_result = await db.execute(check_query, {"tray_no": tray_no})
        tray = tray_result.fetchone()
        if not tray:
            logger.error("Tray not found")
            raise HTTPException(status_code=404, detail={"status":"error","message":"Tray not found"})

        if request.invoice_id:
            # Check if invoice exists before assigning
            exists = await check_invoice_exists(db, request.invoice_id)
            if not exists:
                    logger.error("Invoice not found")
                    raise HTTPException(status_code=404, detail={"status" : "error",
                    "message" : "Invoice Not found"})
        

        # Update current_invoice_no
        update_query = text("""
            UPDATE tray_master
            SET current_invoice_no = :invoice_id
            WHERE tray_no = :tray_no
        """)

        await db.execute(update_query, {
            "invoice_id": request.invoice_id,
            "tray_no": tray_no
        })
        await db.commit()

        logger.info(f"Tray:{tray_no} invoice updated successfully for invoice_id:{request.invoice_id}")
        return {
            "status":"success",
            "message": "Tray invoice updated successfully",
            "tray_no": tray_no,
            "invoice_id": request.invoice_id
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error in update_tray_invoice api: {e}")
        raise HTTPException(
        status_code=500, detail={
            "status": "error",
            "message": str(e).split("\n")[0][:100],
            }
        )
        
        
# @router.put("/scan-quantity")
# async def update_scanned_qty(data: ProductScanUpdate, db: AsyncSession = Depends(get_db), 
#                         current_user: User = Depends(get_current_user)):
#     try:
#         """For updating single product scanned-qty"""
#         if data.invoice_id:
#             # Check if invoice exists before assigning
#             exists = await check_invoice_exists(db, data.invoice_id)
#             if not exists:
#                     logger.error(f"Invoice not found for invoice_id: {data.invoice_id}")
#                     raise HTTPException(status_code=404, detail={"status" : "error",
#                     "message" : f"Invoice not found for invoice_id:{data.invoice_id}"})
                    
#         await scan_quantity_update(db,data,current_user)
#         logger.info("Scanned quantity data updated successfully")
#         return {
#             "status": "success",
#             "message": "Scanned quantity and product data updated successfully",
#         }

#     except HTTPException:
#         await db.rollback()
#         raise
#     except Exception as e:
#         await db.rollback()
#         raise HTTPException(status_code=500, detail={"status": "error", "message": str(e)})
    
    
@router.put("/scan-quantity")
async def update_scanned_qty_products(data: ProductScanQtyUpdate, db: AsyncSession = Depends(get_db), 
                        current_user: User = Depends(get_current_user)):
    try:
        """For updating multiple products scanned-qty"""
        if data.invoice_id:
            # Check if invoice exists before assigning
            exists = await check_invoice_exists(db, data.invoice_id)
            if not exists:
                    logger.error(f"Invoice not found for invoice_id: {data.invoice_id}")
                    raise HTTPException(status_code=404, detail={"status" : "error",
                    "message" : f"Invoice not found for invoice_id: {data.invoice_id}"})
        
        # for product in data.products:
        #     await scan_quantity_update(db, data.invoice_id, product, current_user)
        
        # Concurrency limiter
        if data.completed:
            await release_trays_if_completed(db,data.invoice_id)
        sem = asyncio.Semaphore(4)

        async def limited_scan(product):
            async with sem:
                return await scan_quantity_update_products(db, data.invoice_id, product, current_user)
        tasks = [limited_scan(product) for product in data.products]

        # Run concurrently and collect results safely
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        errors = [str(r) for r in results if isinstance(r, Exception)]
        if errors:
            logger.warning(f"Some product updates failed: {errors}")
            await db.rollback()
            # raise HTTPException(status_code=500, detail={"status": "error", "message":str(errors).split("\n")[0][:200]})
            return {
                "status": "partial_error",
                "message": "Some updates failed",
                "errors": errors,
            }
        
        await db.commit()
        logger.info("Scanned quantity data updated successfully")
        return {
            "status": "success",
            "message": "Scanned quantity and product data updated successfully",
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail={"status": "error", "message":str(e).split("\n")[0][:200]})

    




