from rapidfuzz import process
from src.logger.logger_setup import logger
from fastapi import HTTPException
from sqlalchemy import text
import re
from src.helpers.invoices import update_invoice_status_base_query, invoice_product_exist, invoice_product_list_insert_query, \
    epoch_to_str, invoices_metadata_field_map
from datetime import datetime
import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, Tuple, List
from src.helpers.invoices import FlowType

class Finder:
    """
    Finder class handles product lookup operations using multiple
    search strategies (batch, expiry, MFG, MRP, fuzzy) purely via DB queries.
    """

    def __init__(self, db_session, batch_number, expiry_date,mrp, mfg_date=None, barcode1=None, barcode2=None, rack_id=None):
        self.__db = db_session      # <-- store the session
        self.__batch_number = batch_number or ""
        self.__expiry_date = expiry_date or ""
        self.__mfg_date = mfg_date or ""
        self.__mrp = mrp or ""
        self.__barcode1 = barcode1 or ""
        self.__barcode2 = barcode2 or ""
        self.__rack_id = rack_id
        self.__products = []
        

    async def find_products_by_batch(self):
        """Directly find products by batch number."""
        if not self.__batch_number:
            logger.debug("No batch number provided.")
            return []
        query = """
            SELECT id,item_code,product_name,batch_number,expiry_date,mfg_date,mrp,division,obatch,
            barcode1,barcode2,optional1,optional2 FROM product_master
            WHERE batch_number = :batch
        """
        # self.__products = self.__db.exec_query(query, [self.__batch_number])
        # logger.debug(f"Found {len(self.__products)} products by batch.")
        # return self.__products
        try:
            result = await self.__db.execute(text(query), {"batch": self.__batch_number})
            rows = result.mappings().all()  # returns list[dict]
            self.__products = [dict(row) for row in rows]
            logger.debug(f"Found {len(self.__products)} products by batch: {self.__batch_number}")
            return self.__products

        except Exception as e:
            logger.exception(f"Error finding products by batch: {e}")
            return []

    async def find_products_by_fuzzy_logic(self, skip_mfg=False):
        """Find similar batches using expiry, mfg, and MRP."""
        logger.debug("Inside find_products_by_fuzzy_logic")

        params = {}
        conditions = []
        base_query = """
            SELECT id,item_code,product_name,batch_number,expiry_date,mfg_date,mrp,division,obatch,
            barcode1,barcode2,optional1,optional2
            FROM product_master WHERE
        """

        if self.__expiry_date:
            conditions.append("expiry_date = :expiry_date")
            params["expiry_date"] = self.__expiry_date

        if not skip_mfg and self.__mfg_date:
            conditions.append("mfg_date = :mfg_date")
            params["mfg_date"] = self.__mfg_date

        if self.__mrp:
            try:
                mrp = float(self.__mrp)
                conditions.append("(mrp >= :min_mrp AND mrp <= :max_mrp)")
                params["min_mrp"] = mrp - 1
                params["max_mrp"] = mrp + 1
            except ValueError:
                logger.debug("Invalid MRP value for fuzzy search")

        if len(conditions) < 2:
            logger.debug("Cannot perform fuzzy search with less than 2 parameters")
            return []

        query = base_query + " AND ".join(conditions)
        try:
            # ---  Execute query
            result = await self.__db.execute(text(query), params)
            rows = result.mappings().all()
            self.__products = [dict(row) for row in rows]
            logger.debug(f"Found {len(self.__products)} products in fuzzy search")

            # ---  Fuzzy batch name match
            batches = [p["batch_number"] for p in self.__products if p.get("batch_number")]
            if not batches:
                return self.__products
            scores = process.extract(self.__batch_number, batches)
            found_batches = [batch for (batch, score, _) in scores if score >= 80 and len(batch) > 3]
            if found_batches:
                placeholders = ", ".join([f":b{i}" for i in range(len(found_batches))])
                batch_params = {f"b{i}": b for i, b in enumerate(found_batches)}

                fuzzy_query = query + f" AND batch_number IN ({placeholders})"
                params.update(batch_params)

                result = await self.__db.execute(text(fuzzy_query), params)
                rows = result.mappings().all()
                self.__products = [dict(row) for row in rows]
       
            return self.__products

        except Exception as e:
            logger.exception(f"Error in find_products_by_fuzzy_logic: {e}")
            return []

    def filter_by_mrp(self):
        """Filter by MRP tolerance ±1."""
        logger.debug(f"Filtering by MRP: {self.__mrp}")
        if not self.__mrp:
            return self.__products
        try:
            mrp = float(self.__mrp)
        except ValueError:
            logger.debug("Invalid MRP value for filter_by_mrp")
            return []
        return [
            p for p in self.__products
            if abs(float(p.get("mrp", 0)) - mrp) <= 1
        ]

    def filter_by_expiry(self):
        """Filter by expiry date."""
        logger.debug(f"Filtering by expiry date: {self.__expiry_date}")
        if not self.__expiry_date:
            return self.__products
        # for p in self.__products:
        #     print("product expiry_date",p.get("expiry_date"),p.get("id"))
        return [p for p in self.__products if p.get("expiry_date") == self.__expiry_date]

    def filter_by_mfg(self):
        """Filter by manufacturing date."""
        logger.debug(f"Filtering by MFG date: {self.__mfg_date}")
        if not self.__mfg_date:
            return self.__products
        return [p for p in self.__products if p.get("mfg_date") == self.__mfg_date]
    
    # def filter_by_barcode(self):
    #     """Filter by barcode1 or barcode2 match."""
    #     logger.debug(f"Filtering by barcode: {self.__barcode}")
    #     # If both barcodes are missing, return all products
    #     if not (self.__barcode1 or self.__barcode2):
    #         return self.__products
        
    #     return [
    #         p for p in self.__products
    #         if p.get("barcode1") == self.__barcode1 or p.get("barcode2") == self.__barcode2
    #     ]
    
    async def find_by_barcode(self, barcode: str):
        """
        Find products by barcode, matching either barcode1 or barcode2.
        """
        logger.debug(f"Finding products by barcode: {barcode}")
        if not barcode:
            return []

        if self.__products is None:
            self.__products = []

        query = """
            SELECT id,item_code,product_name,batch_number,expiry_date,mfg_date,mrp,division,obatch,
            barcode1,barcode2,optional1,optional2
            FROM product_master
            WHERE barcode1 = :barcode OR barcode2 = :barcode
        """
        result = await self.__db.execute(text(query), {"barcode": barcode})
        self.__products = result.mappings().all()
        
        logger.debug(f"Found {len(self.__products)} products by barcode")
        return self.__products


    async def check_barcode(self, barcode: str):
        """
        Check if a product exists for a given barcode
        """
        if not barcode:
            return False

        products = await self.find_by_barcode(barcode)

        if len(products) == 1:
            self.__products = products
            logger.info(f"Exact one product found by barcode: {barcode}")
            return True

        logger.debug(f"{len(products)} products found for barcode {barcode}, continuing search flow.")
        return False


    async def search(self):
        """
        Hierarchical search:
        1. Batch → Direct match
        2. Fuzzy → Similar match
        3. Filter → MRP, Expiry, MFG
        """
        logger.debug("Starting search process")

        if self.__barcode1:
            if await self.check_barcode(self.__barcode1):
                return self.__products
            
        if self.__barcode2:
            if await self.check_barcode(self.__barcode2):
                return self.__products
        
        # 1 Try direct batch search
        await self.find_products_by_batch()

        # 2 If none found, try fuzzy search
        if not self.__products:
            tmp_products = await self.find_products_by_fuzzy_logic()
            logger.info(f"products found using find_products_by_fuzzy_logic: {len(tmp_products)}")
            if not tmp_products:
                tmp_products = await self.find_products_by_fuzzy_logic(skip_mfg=True)
                logger.info(f"products found without using mfg find_products_by_fuzzy_logic: {len(tmp_products)}")
            self.__products = tmp_products

        if not self.__products:
            logger.debug("No products found even after fuzzy search")
            return []

        # Apply hierarchical filtering
        filtered = self.__products
        
        mrp_filtered = []
        expiry_filtered = []

        if self.__mrp:
            mrp_filtered = self.filter_by_mrp()
            logger.debug(f"After MRP filter: {len(mrp_filtered)} products")
                
        if (not mrp_filtered or len(mrp_filtered)>5) and self.__expiry_date:
            
            expiry_filtered = self.filter_by_expiry()
            logger.debug(f"After expiry filter: {len(expiry_filtered)} products")
        
        if mrp_filtered or expiry_filtered:
            combined = mrp_filtered + expiry_filtered
            combined_ids = {p.get("id") for p in combined if p.get("id")}
            # Keep only matching ones from original products
            filtered = [p for p in self.__products if p.get("id") in combined_ids]
            logger.debug(f"Matched {len(filtered)} products after MRP/Expiry intersection filter")
        else:
            # If no MRP or Expiry match — return empty
            logger.debug("No products matched either MRP or Expiry")
            return []
        
        # Apply MFG filter only if date present and more than 5 products
        if self.__mfg_date and len(filtered) > 5:
            mfg_filtered = self.filter_by_mfg()
            logger.debug(f"After MFG filter: {len(mfg_filtered)} products")

            if not mfg_filtered:
                # If no MFG matches, retain previous filtered
                logger.debug("No products found by MFG — keeping previous filtered results")
            else:
                # Compare with original filtered results (intersection)
                mfg_ids = {p.get("id") for p in mfg_filtered if p.get("id")}
                intersection = [p for p in filtered if p.get("id") in mfg_ids]

                if intersection:
                    filtered = intersection
                    logger.debug(f"After MFG intersection: {len(filtered)} products retained")
                else:
                    # If intersection empty, retain previous filtered
                    logger.debug("No intersection found — keeping previous filtered results")
            

        logger.debug(f"Final matched {len(filtered)} products")
        return filtered
    
    

async def match_scan(db,data):
    try:
        new_batch = re.sub('[ @#$%^&*()!?-]', "", str(data.batch_number))

        # validate license
        finder = Finder(db,new_batch, data.expiry_date, data.mrp, data.mfg_date, data.barcode1, data.barcode2)
        result = await finder.search()
        if not result or len(result) == 0:
            logger.info(f"No products found for batch: {new_batch}")
            return {
                "status":"error",
                "message":"Product not found",
                "data":[] 
            }
        logger.info(f"Found {len(result)} matching products")
        
        # if len(result) == 1:
        #     product = result[0]
        #     await scan_match_record_entries(db,data,product)
            
        logger.info(f"Found {len(result)} matching products")
        return {
            "status":"success",
            "count_of_products_found":len(result),
            "message":f"Found {len(result)} matching products",
            "data":result
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"inside match_scan {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split("\n")[0][:100]},
        )
        
async def scan_match_record_entries(db,data,product):
    try:
        await db.execute(text(update_invoice_status_base_query), {
                        "invoice_id": data.invoice_id,
                        "updated_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                    })
        
        # Check if product already exists in invoice_product_list
        existing = await db.execute(
            text(invoice_product_exist),
            {
                "invoice_id": data.invoice_id,
                "batch_number": product["batch_number"],
                "expiry_date": product["expiry_date"],
                "mrp": product["mrp"]
            }
        )
        
        row = existing.mappings().first()
        if row:
                # If record exists, increment scanned_qty
                print('record exist in db')
                await db.execute(
                    text("""
                        UPDATE invoice_product_list SET scanned_qty = scanned_qty + 1 WHERE id = :id
                    """),
                    {"id": row["id"]}
                )
                logger.info(f"Incremented scanned_qty for {row['id']}")
        else:
            await db.execute(text(invoice_product_list_insert_query),  {
                        "id": str(uuid.uuid4()),
                        "invoice_id": data.invoice_id,
                        "product_name": product["product_name"],
                        "batch_number": product["batch_number"],
                        "expiry_date": product["expiry_date"],
                        "mrp": product["mrp"],
                        "actual_qty": 0.0,
                        "scanned_qty": 1.0
                    })
        await db.commit()
        #  Transaction and TransactionMetaData data need to add
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Inside scan_match_record_entries: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split("\n")[0][:100]},
        )
    
async def insert_product_qty_converter(db,data,current_user):
    try:
        # Insert new product if not exists
        insert_query = text("""
            INSERT INTO product_qty_converter (
                id, product_name, item_code, shipper_val, box_val, strip_val,
                created_at, updated_at, updated_by
            )
            VALUES (
                :id, :product_name, NULL, :shipper_val, :box_val, :strip_val,
                :created_at, :updated_at, :updated_by
            )
        """)
        await db.execute(
            insert_query,
            {
                "id": str(uuid.uuid4()),
                "product_name": data.product_name,
                "shipper_val": data.shipper_val,
                "box_val": data.box_val,
                "strip_val": data.strip_val,
                "created_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "updated_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "updated_by": current_user.id
            },
        )
        logger.info("Inside insert_product_qty_converter: Inserted product successfully")
    except Exception as e:
        logger.exception(f"Inside insert_product_qty_converter: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split("\n")[0][:100]},
        )

async def compute_qty_updates(data,existing_product):
    try:
        updates={}
        db_shipper = existing_product["shipper_val"] or 0
        db_box = existing_product["box_val"] or 0
        db_strip = existing_product["strip_val"] or 0
        
        if db_shipper > 0 and db_box > 0 and db_strip > 0:
            logger.info(f"{data.product_name} already have box,shipper,strip value: {db_box},{db_shipper},{db_strip}")
            return

        new_shipper = data.shipper_val or 0
        new_box = data.box_val or 0
        new_strip = data.strip_val or 0

        # strip_val always allowed if changed
        if db_strip == 0 and new_strip > 0:
            updates["strip_val"] = new_strip

        if db_box > 0 and db_shipper == 0:
            if new_shipper > 0:
                updates["shipper_val"] = new_shipper

        elif db_box > 0 and db_shipper > 0:
            logger.info(f"Product: {data.product_name} already have box & shipper value: Box:{db_box},Shipper:{db_shipper}")
            pass
        else:
            if new_shipper > 0:
                updates["shipper_val"] = new_shipper
            if new_box > 0:
                updates["box_val"] = new_box
        return updates
    except Exception as e:
        logger.exception(f"Inside compute_qty_updates: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split("\n")[0][:100]},
        )

async def update_product_qty_converter(db,data,existing_product):
    try:
        updates = await compute_qty_updates(data,existing_product)
        
        if updates:
            updates["updated_at"] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            set_clause = ", ".join([f"{col} = :{col}" for col in updates])
            update_query = text(f"""
                UPDATE product_qty_converter
                SET {set_clause}
                WHERE id = :id
            """)
            updates["id"] = existing_product["id"]
            await db.execute(update_query, updates)
            logger.info(f"Inside update_product_qty_converter: Updated product: {data.product_id} successfully")
        logger.info(f"Inside update_product_qty_converter: Not have fields to update for product: {data.product_id}")  
    except Exception as e:
        logger.exception(f"Inside update_product_qty_converter: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split("\n")[0][:100]},
        )


# async def scan_quantity_update(db,data,current_user):
#     try:
#         # Check if product exists in product_qty_converter
#         check_query = text("""
#             SELECT id, shipper_val, box_val, strip_val
#             FROM product_qty_converter
#             WHERE product_name = :product_name
#         """)
#         result = await db.execute(check_query, {"product_name": data.product_name})
#         existing_product = result.mappings().first()

#         if not existing_product:
#             # Insert new product if not exists
#             await insert_product_qty_converter(db,data,current_user)
#         else:
#             # Update product_qty_converter if values differ
#             await update_product_qty_converter(db,data,existing_product)

#         # Update scanned_qty in invoice_product_list
#         update_invoice_query = text("""
#             UPDATE invoice_product_list
#             SET scanned_qty = :scanned_qty
#             WHERE id = :product_id
#             AND invoice_id = :invoice_id
#             AND product_name = :product_name
#         """)
        
#         result = await db.execute(
#             update_invoice_query,
#             {
#                 "invoice_id": data.invoice_id,
#                 "scanned_qty": data.scanned_qty,
#                 "product_name": data.product_name,
#                 "product_id":data.product_id
#             },
#         )
#         if result.rowcount == 0:
#             logger.error(f"No matching product found in invoice {data.invoice_id}")
#             raise HTTPException(
#                 status_code=404,
#                 detail={"status":"error","message":f"No matching product found in invoice {data.invoice_id}"}
#             )

#         await db.commit()
        
#     except HTTPException:
#         await db.rollback()
#         raise
#     except Exception as e:
#         await db.rollback()
#         logger.exception(f"Inside scan_quantity_update: {e}")
#         raise HTTPException(
#             status_code=400,
#             detail={"status": "error", "message": str(e).split("\n")[0][:100]},
#         )

async def scan_quantity_update_products(db,invoice_id,product,flow_type, current_user):
    try:
        
        if flow_type == FlowType.picker:
            scan_qty_key = "picker_scanned_qty"
            scan_status_key = "picker_scan_status"
        else:
            scan_qty_key = "checker_scanned_qty"
            scan_status_key = "checker_scan_status"
        
        # Check if product exists in product_qty_converter
        check_query = text("""
            SELECT id, shipper_val, box_val, strip_val
            FROM product_qty_converter
            WHERE product_name = :product_name
        """)
        result = await db.execute(check_query, {"product_name": product.product_name})
        existing_product = result.mappings().first()

        if not existing_product:
            # Insert new product if not exists
            await insert_product_qty_converter(db,product,current_user)
        else:
            # Update product_qty_converter if values differ
            await update_product_qty_converter(db,product,existing_product)

        # Update scanned_qty in invoice_product_list
        update_invoice_query = text(f"""
            UPDATE invoice_product_list
            SET {scan_qty_key} = :scanned_qty, 
            {scan_status_key} = :scan_status
            WHERE id = :product_id
            AND invoice_id = :invoice_id
            AND product_name = :product_name
        """)
        
        result = await db.execute(
            update_invoice_query,
            {
                "invoice_id": invoice_id,
                "scanned_qty": product.scanned_qty,
                "product_name": product.product_name,
                "product_id":product.product_id,
                "scan_status": product.scan_status.value if product.scan_status else None  
            },
        )
        if result.rowcount == 0:
            logger.error(f"No matching product found in invoice {invoice_id} for {product.product_name}")
            # raise HTTPException(
            #     status_code=404,
            #     detail={"status":"error","message":f"No matching product found in invoice {data.invoice_id}"}
            # )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Inside scan_quantity_update_products: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split("\n")[0][:100]},
        )
        
        
async def release_trays_if_completed(db, invoice_id):
    try:
        # Find trays linked to the invoice
        query = text("""
            SELECT id 
            FROM tray_master
            WHERE current_invoice_no = :invoice_id
        """)
    

        result = await db.execute(query, {"invoice_id": invoice_id})
        trays = result.fetchall()

        if not trays:
            logger.info(f"No trays assigned to invoice {invoice_id}")
            return

        # Set current_invoice_no = NULL for those trays
        update_query = text("""
            UPDATE tray_master
            SET current_invoice_no = NULL
            WHERE current_invoice_no = :invoice_id
        """)

        await db.execute(update_query, {"invoice_id": invoice_id})
        logger.info(f"Released {len(trays)} trays from invoice {invoice_id}")

    except Exception as e:
        logger.exception(f"Error releasing trays: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split("\n")[0][:200]},
        )
        
        
async def get_product_qty_converter_count(
    db: AsyncSession,
    search: Optional[str]
) -> int:
    try:
        base_query = "FROM product_qty_converter"
        where_clause = ""
        params = {}

        if search:
            where_clause = """
                WHERE 
                    LOWER(product_name) LIKE :search
                    OR LOWER(item_code) LIKE :search
            """
            params["search"] = f"%{search.lower()}%"

        count_query = text(f"""
            SELECT COUNT(*) 
            {base_query}
            {where_clause}
        """)

        result = await db.execute(count_query, params)
        return result.scalar()
    except Exception as e:
        logger.error(f"Error get_product_qty_converter_count: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split("\n")[0][:200]},
        )


async def get_product_qty_converter_data(
    db: AsyncSession,
    page: int,
    page_size: int,
    search: Optional[str]
) -> List[dict]:
    try:

        offset = (page - 1) * page_size
        base_query = "FROM product_qty_converter"
        where_clause = ""
        params = {}

        if search:
            where_clause = """
                WHERE 
                    LOWER(product_name) LIKE :search
                    OR LOWER(item_code) LIKE :search
            """
            params["search"] = f"%{search.lower()}%"

        data_query = text(f"""
            SELECT *
            {base_query}
            {where_clause}
            ORDER BY created_at DESC
            LIMIT :limit OFFSET :offset
        """)

        params["limit"] = page_size
        params["offset"] = offset

        result = await db.execute(data_query, params)
        return result.mappings().all()
    except Exception as e:
        logger.error(f"Error get_product_qty_converter_data: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split("\n")[0][:200]},
        )



async def product_qty_converter_exist(db,data):
    try:
        check_query = """
            SELECT id
            FROM product_qty_converter
            WHERE product_name = :product_name
            LIMIT 1;
        """
        result = await db.execute(text(check_query), {
            "product_name": data.product_name
        })
        product = result.mappings().first()

        if not product:
            logger.error(f"Product: {data.product_name} not found")
            raise HTTPException(
                status_code=404,
                detail={"status": "error", "message": f"Product: {data.product_name} not found"}
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Inside product_qty_converter_exist: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split("\n")[0][:100]},
        )
        

async def update_product_qty_converter_values(data,current_user):
    try:
        update_fields = []
        params = {
            "product_name": data.product_name,
            "updated_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            "updated_by": current_user.id
        }

        if data.shipper_val is not None:
            update_fields.append("shipper_val = :shipper_val")
            params["shipper_val"] = data.shipper_val

        if data.box_val is not None:
            update_fields.append("box_val = :box_val")
            params["box_val"] = data.box_val

        if data.strip_val is not None:
            update_fields.append("strip_val = :strip_val")
            params["strip_val"] = data.strip_val

        # If nothing to update
        if not update_fields:
            raise HTTPException(
                status_code=400,
                detail={"status": "error", "message": "No values provided to update"}
            )
        return update_fields, params
            
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Inside product_qty_converter_exist: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split("\n")[0][:100]},
        )
        
    