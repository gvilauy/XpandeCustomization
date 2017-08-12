/******************************************************************************
 * Product: Adempiere ERP & CRM Smart Business Solution                       *
 * Copyright (C) 1999-2006 ComPiere, Inc. All Rights Reserved.                *
 * This program is free software; you can redistribute it and/or modify it    *
 * under the terms version 2 of the GNU General Public License as published   *
 * by the Free Software Foundation. This program is distributed in the hope   *
 * that it will be useful, but WITHOUT ANY WARRANTY; without even the implied *
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.           *
 * See the GNU General Public License for more details.                       *
 * You should have received a copy of the GNU General Public License along    *
 * with this program; if not, write to the Free Software Foundation, Inc.,    *
 * 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.                     *
 * For the text or an alternative of this public license, you may reach us    *
 * ComPiere, Inc., 2620 Augustine Dr. #245, Santa Clara, CA 95054, USA        *
 * or via info@compiere.org or http://www.compiere.org/license.html           *
 *****************************************************************************/
package org.compiere.model;

import dto.sisteco.SistecoConvertResponse;
import dto.sisteco.SistecoResponseDTO;
import dto.uy.gub.dgi.cfe.*;
import org.adempiere.exceptions.AdempiereException;
import org.adempiere.exceptions.BPartnerNoAddressException;
import org.adempiere.exceptions.DBException;
import org.apache.axis.client.Call;
import org.apache.axis.client.Service;
import org.apache.axis.encoding.XMLType;
import org.compiere.acct.Doc;
import org.compiere.print.ReportEngine;
import org.compiere.process.DocAction;
import org.compiere.process.DocOptions;
import org.compiere.process.DocumentEngine;
import org.compiere.util.*;
import org.eevolution.model.MPPProductBOM;
import org.eevolution.model.MPPProductBOMLine;
import org.xpande.cfe.model.MZCFERespuestaProvider;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.namespace.QName;
import javax.xml.rpc.ParameterMode;
import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;


/**
 *	Invoice Model.
 * 	Please do not set DocStatus and C_DocType_ID directly.
 * 	They are set in the process() method.
 * 	Use DocAction and C_DocTypeTarget_ID instead.
 *
 *  @author Jorg Janke
 *  @version $Id: MInvoice.java,v 1.2 2006/07/30 00:51:02 jjanke Exp $
 *  @author victor.perez@e-evolution.com, e-Evolution http://www.e-evolution.com
 *  		@see ://sourceforge.net/tracker/?func=detail&atid=879335&aid=1948157&group_id=176962
 * 			<li> FR [ 2520591 ] Support multiples calendar for Org
 *			@see ://sourceforge.net/tracker2/?func=detail&atid=879335&aid=2520591&group_id=176962
 *  Modifications Added RMA functionality (Ashley Ramdass)
 *  @author Yamel Senih, ysenih@erpcya.com, ERPCyA http://www.erpcya.com
 *			<a href="https://github.com/adempiere/adempiere/issues/887">
 * 			@see  [ 887 ] System Config reversal invoice DocNo</a>
 */
public class MInvoice extends X_C_Invoice implements DocAction, DocOptions {

	// Xpande. Momentaneo para CFE
	private BigDecimal montoC124 = Env.ZERO;
	// Xpande.

	/**
	 * 
	 */
	private static final long serialVersionUID = 816227083897031327L;

	/***
	 * Actiones de documentos customizadas.
	 * Xpande. Created by Gabriel Vila on 8/3/17. Issue #4.
	 * @param docStatus
	 * @param processing
	 * @param orderType
	 * @param isSOTrx
	 * @param AD_Table_ID
	 * @param docAction
	 * @param options
	 * @param index
	 * @return
	 */
	@Override
	public int customizeValidActions(String docStatus, Object processing, String orderType, String isSOTrx, int AD_Table_ID, String[] docAction, String[] options, int index) {

		int newIndex = 0;

		if ((docStatus.equalsIgnoreCase(STATUS_Drafted))
				|| (docStatus.equalsIgnoreCase(STATUS_Invalid))
				|| (docStatus.equalsIgnoreCase(STATUS_InProgress))){

			options[newIndex++] = DocumentEngine.ACTION_Prepare;
			options[newIndex++] = DocumentEngine.ACTION_Complete;

		}
		else if (docStatus.equalsIgnoreCase(STATUS_Completed)){

			// En invoices, solo permito reactivar comprobantes de compra.
			if (isSOTrx.equalsIgnoreCase("N")){
				options[newIndex++] = DocumentEngine.ACTION_ReActivate;
			}

			// No tiene sentido anular en comprobantes de compra, porque se puede hacer lo mismo reactivandolo y eliminandolo.
			// No es posible anular comprobantes de venta, debido a CFE.
			//options[newIndex++] = DocumentEngine.ACTION_Void;
		}

		return newIndex;
	} // Xpande.



	/**
	 * 	Get Payments Of BPartner
	 *	@param ctx context
	 *	@param C_BPartner_ID id
	 *	@param trxName transaction
	 *	@return array
	 */
	public static MInvoice[] getOfBPartner (Properties ctx, int C_BPartner_ID, String trxName)
	{
		List<MInvoice> list = new Query(ctx, Table_Name, COLUMNNAME_C_BPartner_ID+"=?", trxName)
									.setParameters(C_BPartner_ID)
									.list();
		return list.toArray(new MInvoice[list.size()]);
	}	//	getOfBPartner

	/**
	 * 	Create new Invoice by copying
	 * 	@param from invoice
	 * 	@param dateDoc date of the document date
	 * 	@param C_DocTypeTarget_ID target doc type
	 * 	@param isSOTrx sales order
	 * 	@param counter create counter links
	 * 	@param isReversal is a reversal document
	 * 	@param trxName trx
	 * 	@param setOrder set Order links
	 *	@return Invoice
	 */
	public static MInvoice copyFrom (MInvoice from, Timestamp dateDoc, Timestamp dateAcct,
		int C_DocTypeTarget_ID, boolean isSOTrx, boolean counter, boolean isReversal,
		String trxName, boolean setOrder)
	{
		MInvoice to = new MInvoice (from.getCtx(), 0, trxName);
		PO.copyValues (from, to, from.getAD_Client_ID(), from.getAD_Org_ID());
		to.set_ValueNoCheck ("C_Invoice_ID", I_ZERO);
		to.set_ValueNoCheck("DocumentNo", null);
		//	For Reversal
		if(isReversal) {
			to.setReversal(true);
			to.setReversal_ID(from.getC_Invoice_ID());
			MDocType docType = MDocType.get(from.getCtx(), from.getC_DocType_ID());
			//	Set Document No from flag
			if(docType.isCopyDocNoOnReversal()) {
				to.setDocumentNo(from.getDocumentNo() + "^");
			}
		}
		//
		to.setDocStatus (DOCSTATUS_Drafted);		//	Draft
		to.setDocAction(DOCACTION_Complete);
		//
		to.setC_DocType_ID(0);
		to.setC_DocTypeTarget_ID (C_DocTypeTarget_ID);
		to.setIsSOTrx(isSOTrx);
		//
		to.setDateInvoiced (dateDoc);
		to.setDateAcct (dateAcct);
		to.setDatePrinted(null);
		to.setIsPrinted (false);
		//
		to.setIsApproved (false);
		to.setC_Payment_ID(0);
		to.setC_CashLine_ID(0);
		to.setIsPaid (false);
		to.setIsInDispute(false);
		//
		//	Amounts are updated by trigger when adding lines
		to.setGrandTotal(Env.ZERO);
		to.setTotalLines(Env.ZERO);
		//
		to.setIsTransferred (false);
		to.setPosted (false);
		to.setProcessed (false);
		//[ 1633721 ] Reverse Documents- Processing=Y
		to.setProcessing(false);
		//	delete references
		to.setIsSelfService(false);
		if (!setOrder)
			to.setC_Order_ID(0);
		if (counter)
		{
			to.setRef_Invoice_ID(from.getC_Invoice_ID());
			//	Try to find Order link
			if (from.getC_Order_ID() != 0)
			{
				MOrder peer = new MOrder (from.getCtx(), from.getC_Order_ID(), from.get_TrxName());
				if (peer.getRef_Order_ID() != 0)
					to.setC_Order_ID(peer.getRef_Order_ID());
			}
			// Try to find RMA link
			if (from.getM_RMA_ID() != 0)
			{
				MRMA peer = new MRMA (from.getCtx(), from.getM_RMA_ID(), from.get_TrxName());
				if (peer.getRef_RMA_ID() > 0)
					to.setM_RMA_ID(peer.getRef_RMA_ID());
			}
			//
		} else {
			to.setRef_Invoice_ID(0);
		}
		//	Save Copied
		to.saveEx(trxName);
		if (counter)
			from.setRef_Invoice_ID(to.getC_Invoice_ID());

		//	Lines
		// Check lines exist before copy
		if (from.getLines(true).length > 0) {
			if (to.copyLinesFrom(from, counter, setOrder) == 0)
				throw new IllegalStateException("Could not create Invoice Lines");
		}	

		return to;
	}

	/**
	 * 	Get PDF File Name
	 *	@param documentDir directory
	 * 	@param C_Invoice_ID invoice
	 *	@return file name
	 */
	public static String getPDFFileName (String documentDir, int C_Invoice_ID)
	{
		StringBuffer sb = new StringBuffer (documentDir);
		if (sb.length() == 0)
			sb.append(".");
		if (!sb.toString().endsWith(File.separator))
			sb.append(File.separator);
		sb.append("C_Invoice_ID_")
			.append(C_Invoice_ID)
			.append(".pdf");
		return sb.toString();
	}	//	getPDFFileName


	/**
	 * 	Get MInvoice from Cache
	 *	@param ctx context
	 *	@param C_Invoice_ID id
	 *	@return MInvoice
	 */
	public static MInvoice get (Properties ctx, int C_Invoice_ID)
	{
		Integer key = new Integer (C_Invoice_ID);
		MInvoice retValue = (MInvoice) s_cache.get (key);
		if (retValue != null)
			return retValue;
		retValue = new MInvoice (ctx, C_Invoice_ID, null);
		if (retValue.get_ID () != 0)
			s_cache.put (key, retValue);
		return retValue;
	} //	get

	/**	Cache						*/
	private static CCache<Integer,MInvoice>	s_cache	= new CCache<Integer,MInvoice>("C_Invoice", 20, 2);	//	2 minutes


	/**************************************************************************
	 * 	Invoice Constructor
	 * 	@param ctx context
	 * 	@param C_Invoice_ID invoice or 0 for new
	 * 	@param trxName trx name
	 */
	public MInvoice(Properties ctx, int C_Invoice_ID, String trxName)
	{
		super (ctx, C_Invoice_ID, trxName);
		if (C_Invoice_ID == 0)
		{
			setDocStatus (DOCSTATUS_Drafted);		//	Draft
			setDocAction (DOCACTION_Complete);
			//
			setPaymentRule(PAYMENTRULE_OnCredit);	//	Payment Terms

			setDateInvoiced (new Timestamp (System.currentTimeMillis ()));
			setDateAcct (new Timestamp (System.currentTimeMillis ()));
			//
			setChargeAmt (Env.ZERO);
			setTotalLines (Env.ZERO);
			setGrandTotal (Env.ZERO);
			//
			setIsSOTrx (true);
			setIsTaxIncluded (false);
			setIsApproved (false);
			setIsDiscountPrinted (false);
			setIsPaid (false);
			setSendEMail (false);
			setIsPrinted (false);
			setIsTransferred (false);
			setIsSelfService(false);
			setIsPayScheduleValid(false);
			setIsInDispute(false);
			setPosted(false);
			super.setProcessed (false);
			setProcessing(false);
		}
	}	//	MInvoice

	/**
	 *  Load Constructor
	 *  @param ctx context
	 *  @param rs result set record
	 *	@param trxName transaction
	 */
	public MInvoice(Properties ctx, ResultSet rs, String trxName)
	{
		super(ctx, rs, trxName);
	}	//	MInvoice

	/**
	 * 	Create Invoice from Order
	 *	@param order order
	 *	@param C_DocTypeTarget_ID target document type
	 *	@param invoiceDate date or null
	 */
	public MInvoice(MOrder order, int C_DocTypeTarget_ID, Timestamp invoiceDate)
	{
		this (order.getCtx(), 0, order.get_TrxName());
		setClientOrg(order);
		setOrder(order);	//	set base settings
		//
		if (C_DocTypeTarget_ID <= 0)
		{
			MDocType odt = MDocType.get(order.getCtx(), order.getC_DocType_ID());
			if (odt != null)
			{
				C_DocTypeTarget_ID = odt.getC_DocTypeInvoice_ID();
				if (C_DocTypeTarget_ID <= 0)
					throw new AdempiereException("@NotFound@ @C_DocTypeInvoice_ID@ - @C_DocType_ID@:"+odt.get_Translation(MDocType.COLUMNNAME_Name));
			}
		}
		setC_DocTypeTarget_ID(C_DocTypeTarget_ID);
		if (invoiceDate != null)
			setDateInvoiced(invoiceDate);
		setDateAcct(getDateInvoiced());
		//
		setSalesRep_ID(order.getSalesRep_ID());
		//
		setC_BPartner_ID(order.getBill_BPartner_ID());
		setC_BPartner_Location_ID(order.getBill_Location_ID());
		setAD_User_ID(order.getBill_User_ID());
	}	//	MInvoice

	/**
	 * 	Create Invoice from Shipment
	 *	@param ship shipment
	 *	@param invoiceDate date or null
	 */
	public MInvoice(MInOut ship, Timestamp invoiceDate)
	{
		this (ship.getCtx(), 0, ship.get_TrxName());
		setClientOrg(ship);
		setShipment(ship);	//	set base settings
		//
		setC_DocTypeTarget_ID();
		if (invoiceDate != null)
			setDateInvoiced(invoiceDate);
		setDateAcct(getDateInvoiced());
		//
		setSalesRep_ID(ship.getSalesRep_ID());
	}	//	MInvoice

	/**
	 * 	Create Invoice from Batch Line
	 *	@param batch batch
	 *	@param line batch line
	 */
	public MInvoice(MInvoiceBatch batch, MInvoiceBatchLine line)
	{
		this (line.getCtx(), 0, line.get_TrxName());
		setClientOrg(line);
		setDocumentNo(line.getDocumentNo());
		//
		setIsSOTrx(batch.isSOTrx());
		MBPartner bp = new MBPartner (line.getCtx(), line.getC_BPartner_ID(), line.get_TrxName());
		setBPartner(bp);	//	defaults
		//
		setIsTaxIncluded(line.isTaxIncluded());
		//	May conflict with default price list
		setC_Currency_ID(batch.getC_Currency_ID());
		setC_ConversionType_ID(batch.getC_ConversionType_ID());
		//
	//	setPaymentRule(order.getPaymentRule());
	//	setC_PaymentTerm_ID(order.getC_PaymentTerm_ID());
	//	setPOReference("");
		setDescription(batch.getDescription());
	//	setDateOrdered(order.getDateOrdered());
		//
		setAD_OrgTrx_ID(line.getAD_OrgTrx_ID());
		setC_Project_ID(line.getC_Project_ID());
	//	setC_Campaign_ID(line.getC_Campaign_ID());
		setC_Activity_ID(line.getC_Activity_ID());
		setUser1_ID(line.getUser1_ID());
		setUser2_ID(line.getUser2_ID());
		setUser3_ID(line.getUser3_ID());
		setUser4_ID(line.getUser4_ID());
		//
		setC_DocTypeTarget_ID(line.getC_DocType_ID());
		setDateInvoiced(line.getDateInvoiced());
		setDateAcct(line.getDateAcct());
		//
		setSalesRep_ID(batch.getSalesRep_ID());
		//
		setC_BPartner_ID(line.getC_BPartner_ID());
		setC_BPartner_Location_ID(line.getC_BPartner_Location_ID());
		setAD_User_ID(line.getAD_User_ID());
	}	//	MInvoice

	/**	Open Amount				*/
	private BigDecimal 		m_openAmt = null;

	/**	Invoice Lines			*/
	private MInvoiceLine[]	m_lines;
	/**	Invoice Taxes			*/
	private MInvoiceTax[]	m_taxes;
	/**	Logger			*/
	private static CLogger s_log = CLogger.getCLogger(MInvoice.class);

	/**
	 * 	Overwrite Client/Org if required
	 * 	@param AD_Client_ID client
	 * 	@param AD_Org_ID org
	 */
	public void setClientOrg (int AD_Client_ID, int AD_Org_ID)
	{
		super.setClientOrg(AD_Client_ID, AD_Org_ID);
	}	//	setClientOrg

	/**
	 * 	Set Business Partner Defaults & Details
	 * 	@param bp business partner
	 */
	public void setBPartner (MBPartner bp)
	{
		if (bp == null)
			return;

		setC_BPartner_ID(bp.getC_BPartner_ID());
		//	Set Defaults
		int ii = 0;
		if (isSOTrx())
			ii = bp.getC_PaymentTerm_ID();
		else
			ii = bp.getPO_PaymentTerm_ID();
		if (ii != 0)
			setC_PaymentTerm_ID(ii);
		//
		if (isSOTrx())
			ii = bp.getM_PriceList_ID();
		else
			ii = bp.getPO_PriceList_ID();
		if (ii != 0)
			setM_PriceList_ID(ii);
		//
		String ss = bp.getPaymentRule();
		if (ss != null)
			setPaymentRule(ss);


		//	Set Locations
		MBPartnerLocation[] locs = bp.getLocations(false);
		if (locs != null)
		{
			for (int i = 0; i < locs.length; i++)
			{
				if ((locs[i].isBillTo() && isSOTrx())
				|| (locs[i].isPayFrom() && !isSOTrx()))
					setC_BPartner_Location_ID(locs[i].getC_BPartner_Location_ID());
			}
			//	set to first
			if (getC_BPartner_Location_ID() == 0 && locs.length > 0)
				setC_BPartner_Location_ID(locs[0].getC_BPartner_Location_ID());
		}
		if (getC_BPartner_Location_ID() == 0)
			log.log(Level.SEVERE, new BPartnerNoAddressException(bp).getLocalizedMessage()); //TODO: throw exception?

		//	Set Contact
		MUser[] contacts = bp.getContacts(false);
		if (contacts != null && contacts.length > 0)	//	get first User
			setAD_User_ID(contacts[0].getAD_User_ID());
	}	//	setBPartner

	/**
	 * 	Set Order References
	 * 	@param order order
	 */
	public void setOrder (MOrder order)
	{
		if (order == null)
			return;

		setC_Order_ID(order.getC_Order_ID());
		setIsSOTrx(order.isSOTrx());
		setIsDiscountPrinted(order.isDiscountPrinted());
		setIsSelfService(order.isSelfService());
		setSendEMail(order.isSendEMail());
		//
		setM_PriceList_ID(order.getM_PriceList_ID());
		setIsTaxIncluded(order.isTaxIncluded());
		setC_Currency_ID(order.getC_Currency_ID());
		setC_ConversionType_ID(order.getC_ConversionType_ID());
		//
		setPaymentRule(order.getPaymentRule());
		setC_PaymentTerm_ID(order.getC_PaymentTerm_ID());
		setPOReference(order.getPOReference());
		setDescription(order.getDescription());
		setDateOrdered(order.getDateOrdered());
		//
		setAD_OrgTrx_ID(order.getAD_OrgTrx_ID());
		setC_Project_ID(order.getC_Project_ID());
		setC_Campaign_ID(order.getC_Campaign_ID());
		setC_Activity_ID(order.getC_Activity_ID());
		setUser1_ID(order.getUser1_ID());
		setUser2_ID(order.getUser2_ID());
		setUser3_ID(order.getUser3_ID());
		setUser4_ID(order.getUser4_ID());
	}	//	setOrder

	/**
	 * 	Set Shipment References
	 * 	@param ship shipment
	 */
	public void setShipment (MInOut ship)
	{
		if (ship == null)
			return;

		setIsSOTrx(ship.isSOTrx());
		//
		MBPartner bp = new MBPartner (getCtx(), ship.getC_BPartner_ID(), null);
		setBPartner (bp);
		//
		setAD_User_ID(ship.getAD_User_ID());
		//
		setSendEMail(ship.isSendEMail());
		//
		setPOReference(ship.getPOReference());
		setDescription(ship.getDescription());
		setDateOrdered(ship.getDateOrdered());
		//
		setAD_OrgTrx_ID(ship.getAD_OrgTrx_ID());
		setC_Project_ID(ship.getC_Project_ID());
		setC_Campaign_ID(ship.getC_Campaign_ID());
		setC_Activity_ID(ship.getC_Activity_ID());
		setUser1_ID(ship.getUser1_ID());
		setUser2_ID(ship.getUser2_ID());
		setUser3_ID(ship.getUser3_ID());
		setUser4_ID(ship.getUser4_ID());
		//
		if (ship.getC_Order_ID() != 0)
		{
			setC_Order_ID(ship.getC_Order_ID());
			MOrder order = new MOrder (getCtx(), ship.getC_Order_ID(), get_TrxName());
			setIsDiscountPrinted(order.isDiscountPrinted());
			setM_PriceList_ID(order.getM_PriceList_ID());
			setIsTaxIncluded(order.isTaxIncluded());
			setC_Currency_ID(order.getC_Currency_ID());
			setC_ConversionType_ID(order.getC_ConversionType_ID());
			setPaymentRule(order.getPaymentRule());
			setC_PaymentTerm_ID(order.getC_PaymentTerm_ID());
			//
			MDocType dt = MDocType.get(getCtx(), order.getC_DocType_ID());
			if (dt.getC_DocTypeInvoice_ID() != 0)
				setC_DocTypeTarget_ID(dt.getC_DocTypeInvoice_ID());
			// Overwrite Invoice BPartner
			setC_BPartner_ID(order.getBill_BPartner_ID());
			// Overwrite Invoice Address
			setC_BPartner_Location_ID(order.getBill_Location_ID());
			// Overwrite Contact
			setAD_User_ID(order.getBill_User_ID());
			//
		}
        // Check if Shipment/Receipt is based on RMA
        if (ship.getM_RMA_ID() != 0)
        {
            setM_RMA_ID(ship.getM_RMA_ID());

            MRMA rma = new MRMA(getCtx(), ship.getM_RMA_ID(), get_TrxName());
            // Retrieves the invoice DocType
            MDocType dt = MDocType.get(getCtx(), rma.getC_DocType_ID());
            if (dt.getC_DocTypeInvoice_ID() != 0)
            {
                setC_DocTypeTarget_ID(dt.getC_DocTypeInvoice_ID());
            }
            setIsSOTrx(rma.isSOTrx());

            MOrder rmaOrder = rma.getOriginalOrder();
            if (rmaOrder != null) {
                setM_PriceList_ID(rmaOrder.getM_PriceList_ID());
                setIsTaxIncluded(rmaOrder.isTaxIncluded());
                setC_Currency_ID(rmaOrder.getC_Currency_ID());
                setC_ConversionType_ID(rmaOrder.getC_ConversionType_ID());
                setPaymentRule(rmaOrder.getPaymentRule());
                setC_PaymentTerm_ID(rmaOrder.getC_PaymentTerm_ID());
                setC_BPartner_Location_ID(rmaOrder.getBill_Location_ID());
            }
        }

	}	//	setShipment

	/**
	 * 	Set Target Document Type
	 * 	@param DocBaseType doc type MDocType.DOCBASETYPE_
	 */
	public void setC_DocTypeTarget_ID (String DocBaseType)
	{
		String sql = "SELECT C_DocType_ID FROM C_DocType "
			+ "WHERE AD_Client_ID=? AND AD_Org_ID in (0,?) AND DocBaseType=?"
			+ " AND IsActive='Y' "
			+ "ORDER BY IsDefault DESC, AD_Org_ID DESC";
		int C_DocType_ID = DB.getSQLValueEx(null, sql, getAD_Client_ID(), getAD_Org_ID(), DocBaseType);
		if (C_DocType_ID <= 0)
			log.log(Level.SEVERE, "Not found for AD_Client_ID="
				+ getAD_Client_ID() + " - " + DocBaseType);
		else
		{
			log.fine(DocBaseType);
			setC_DocTypeTarget_ID (C_DocType_ID);
			boolean isSOTrx = MDocType.DOCBASETYPE_ARInvoice.equals(DocBaseType)
				|| MDocType.DOCBASETYPE_ARCreditMemo.equals(DocBaseType);
			setIsSOTrx (isSOTrx);
		}
	}	//	setC_DocTypeTarget_ID

	/**
	 * 	Set Target Document Type.
	 * 	Based on SO flag AP/AP Invoice
	 */
	public void setC_DocTypeTarget_ID ()
	{
		if (getC_DocTypeTarget_ID() > 0)
			return;
		if (isSOTrx())
			setC_DocTypeTarget_ID(MDocType.DOCBASETYPE_ARInvoice);
		else
			setC_DocTypeTarget_ID(MDocType.DOCBASETYPE_APInvoice);
	}	//	setC_DocTypeTarget_ID


	/**
	 * 	Get Grand Total
	 * 	@param creditMemoAdjusted adjusted for CM (negative)
	 *	@return grand total
	 */
	public BigDecimal getGrandTotal (boolean creditMemoAdjusted)
	{
		if (!creditMemoAdjusted)
			return super.getGrandTotal();
		//
		BigDecimal amt = getGrandTotal();
		if (isCreditMemo())
			return amt.negate();
		return amt;
	}	//	getGrandTotal


	/**
	 * 	Get Invoice Lines of Invoice
	 * 	@param whereClause starting with AND
	 * 	@return lines
	 */
	private MInvoiceLine[] getLines (String whereClause)
	{
		String whereClauseFinal = "C_Invoice_ID=? ";
		if (whereClause != null)
			whereClauseFinal += whereClause;
		List<MInvoiceLine> list = new Query(getCtx(), I_C_InvoiceLine.Table_Name, whereClauseFinal, get_TrxName())
										.setParameters(getC_Invoice_ID())
										.setOrderBy(I_C_InvoiceLine.COLUMNNAME_Line)
										.list();
		return list.toArray(new MInvoiceLine[list.size()]);
	}	//	getLines

	/**
	 * 	Get Invoice Lines
	 * 	@param requery
	 * 	@return lines
	 */
	public MInvoiceLine[] getLines (boolean requery)
	{
		if (m_lines == null || m_lines.length == 0 || requery)
			m_lines = getLines(null);
		set_TrxName(m_lines, get_TrxName());
		return m_lines;
	}	//	getLines

	/**
	 * 	Get Lines of Invoice
	 * 	@return lines
	 */
	public MInvoiceLine[] getLines()
	{
		return getLines(false);
	}	//	getLines


	/**
	 * 	Renumber Lines
	 *	@param step start and step
	 */
	public void renumberLines (int step)
	{
		int number = step;
		MInvoiceLine[] lines = getLines(false);
		for (int i = 0; i < lines.length; i++)
		{
			MInvoiceLine line = lines[i];
			line.setLine(number);
			line.saveEx();
			number += step;
		}
		m_lines = null;
	}	//	renumberLines

	/**
	 * 	Copy Lines From other Invoice.
	 *	@param otherInvoice invoice
	 * 	@param counter create counter links
	 * 	@param setOrder set order links
	 *	@return number of lines copied
	 */
	public int copyLinesFrom (MInvoice otherInvoice, boolean counter, boolean setOrder)
	{
		if (isProcessed() || isPosted() || otherInvoice == null)
			return 0;
		MInvoiceLine[] fromLines = otherInvoice.getLines(false);
		int count = 0;
		for (int i = 0; i < fromLines.length; i++)
		{
			MInvoiceLine line = new MInvoiceLine (getCtx(), 0, get_TrxName());
			MInvoiceLine fromLine = fromLines[i];
			if (counter)	//	header
				PO.copyValues (fromLine, line, getAD_Client_ID(), getAD_Org_ID());
			else
				PO.copyValues (fromLine, line, fromLine.getAD_Client_ID(), fromLine.getAD_Org_ID());
			line.setC_Invoice_ID(getC_Invoice_ID());
			line.setInvoice(this);
			line.set_ValueNoCheck ("C_InvoiceLine_ID", I_ZERO);	// new
			//	Reset
			if (!setOrder)
				line.setC_OrderLine_ID(0);
			line.setRef_InvoiceLine_ID(0);
			line.setM_InOutLine_ID(0);
			line.setA_Asset_ID(0);
			line.setM_AttributeSetInstance_ID(0);
			line.setS_ResourceAssignment_ID(0);
			//	New Tax
			if (getC_BPartner_ID() != otherInvoice.getC_BPartner_ID())
				line.setTax();	//	recalculate
			//
			if (counter)
			{
				line.setRef_InvoiceLine_ID(fromLine.getC_InvoiceLine_ID());
				if (fromLine.getC_OrderLine_ID() != 0)
				{
					MOrderLine peer = new MOrderLine (getCtx(), fromLine.getC_OrderLine_ID(), get_TrxName());
					if (peer.getRef_OrderLine_ID() != 0)
						line.setC_OrderLine_ID(peer.getRef_OrderLine_ID());
				}
				line.setM_InOutLine_ID(0);
				if (fromLine.getM_InOutLine_ID() != 0)
				{
					MInOutLine peer = new MInOutLine (getCtx(), fromLine.getM_InOutLine_ID(), get_TrxName());
					if (peer.getRef_InOutLine_ID() != 0)
						line.setM_InOutLine_ID(peer.getRef_InOutLine_ID());
				}
			}
			//
			line.setProcessed(false);
			if (line.save(get_TrxName()))
				count++;
			//	Cross Link
			if (counter)
			{
				fromLine.setRef_InvoiceLine_ID(line.getC_InvoiceLine_ID());
				fromLine.save(get_TrxName());
			}

			// MZ Goodwill
			// copy the landed cost
			line.copyLandedCostFrom(fromLine);
			line.allocateLandedCosts();
			// end MZ
		}
		if (fromLines.length != count)
			log.log(Level.SEVERE, "Line difference - From=" + fromLines.length + " <> Saved=" + count);
		return count;
	}	//	copyLinesFrom

	/** Reversal Flag		*/
	private boolean m_reversal = false;

	/**
	 * 	Set Reversal
	 *	@param reversal reversal
	 */
	private void setReversal(boolean reversal)
	{
		m_reversal = reversal;
	}	//	setReversal
	/**
	 * 	Is Reversal
	 *	@return reversal
	 */
	public boolean isReversal()
	{
		return m_reversal;
	}	//	isReversal

	/**
	 * 	Get Taxes
	 *	@param requery requery
	 *	@return array of taxes
	 */
	public MInvoiceTax[] getTaxes (boolean requery)
	{
		if (m_taxes != null && !requery)
			return m_taxes;

		final String whereClause = MInvoiceTax.COLUMNNAME_C_Invoice_ID+"=?";
		List<MInvoiceTax> list = new Query(getCtx(), I_C_InvoiceTax.Table_Name, whereClause, get_TrxName())
										.setParameters(get_ID())
										.list();
		m_taxes = list.toArray(new MInvoiceTax[list.size()]);
		return m_taxes;
	}	//	getTaxes

	/**
	 * 	Add to Description
	 *	@param description text
	 */
	public void addDescription (String description)
	{
		String desc = getDescription();
		if (desc == null)
			setDescription(description);
		else
			setDescription(desc + " | " + description);
	}	//	addDescription

	/**
	 * 	Is it a Credit Memo?
	 *	@return true if CM
	 */
	public boolean isCreditMemo()
	{
		MDocType dt = MDocType.get(getCtx(),
			getC_DocType_ID()==0 ? getC_DocTypeTarget_ID() : getC_DocType_ID());
		return MDocType.DOCBASETYPE_APCreditMemo.equals(dt.getDocBaseType())
			|| MDocType.DOCBASETYPE_ARCreditMemo.equals(dt.getDocBaseType());
	}	//	isCreditMemo

	/**
	 * 	Set Processed.
	 * 	Propergate to Lines/Taxes
	 *	@param processed processed
	 */
	public void setProcessed (boolean processed)
	{
		super.setProcessed (processed);
		if (get_ID() == 0)
			return;
		String set = "SET Processed='"
			+ (processed ? "Y" : "N")
			+ "' WHERE C_Invoice_ID=" + getC_Invoice_ID();
		int noLine = DB.executeUpdate("UPDATE C_InvoiceLine " + set, get_TrxName());
		int noTax = DB.executeUpdate("UPDATE C_InvoiceTax " + set, get_TrxName());
		m_lines = null;
		m_taxes = null;
		log.fine(processed + " - Lines=" + noLine + ", Tax=" + noTax);
	}	//	setProcessed

	/**
	 * 	Validate Invoice Pay Schedule
	 *	@return pay schedule is valid
	 */
	public boolean validatePaySchedule()
	{
		MInvoicePaySchedule[] schedule = MInvoicePaySchedule.getInvoicePaySchedule
			(getCtx(), getC_Invoice_ID(), 0, get_TrxName());
		log.fine("#" + schedule.length);
		if (schedule.length == 0)
		{
			setIsPayScheduleValid(false);
			return false;
		}
		//	Add up due amounts
		BigDecimal total = Env.ZERO;
		for (int i = 0; i < schedule.length; i++)
		{
			schedule[i].setParent(this);
			BigDecimal due = schedule[i].getDueAmt();
			if (due != null)
				total = total.add(due);
		}
		boolean valid = getGrandTotal().compareTo(total) == 0;
		setIsPayScheduleValid(valid);

		//	Update Schedule Lines
		for (int i = 0; i < schedule.length; i++)
		{
			if (schedule[i].isValid() != valid)
			{
				schedule[i].setIsValid(valid);
				schedule[i].saveEx(get_TrxName());
			}
		}
		return valid;
	}	//	validatePaySchedule


	/**************************************************************************
	 * 	Before Save
	 *	@param newRecord new
	 *	@return true
	 */
	protected boolean beforeSave (boolean newRecord)
	{
		log.fine("");
		//	No Partner Info - set Template
		if (getC_BPartner_ID() == 0)
			setBPartner(MBPartner.getTemplate(getCtx(), getAD_Client_ID()));
		if (getC_BPartner_Location_ID() == 0)
			setBPartner(new MBPartner(getCtx(), getC_BPartner_ID(), get_TrxName()));

		//	Price List
		if (getM_PriceList_ID() == 0)
		{
			int ii = Env.getContextAsInt(getCtx(), "#M_PriceList_ID");
			if (ii != 0)
				setM_PriceList_ID(ii);
			else
			{
				String sql = "SELECT M_PriceList_ID FROM M_PriceList WHERE AD_Client_ID=? AND IsDefault='Y'";
				ii = DB.getSQLValue (null, sql, getAD_Client_ID());
				if (ii != 0)
					setM_PriceList_ID (ii);
			}
		}

		//	Currency
		if (getC_Currency_ID() == 0)
		{
			String sql = "SELECT C_Currency_ID FROM M_PriceList WHERE M_PriceList_ID=?";
			int ii = DB.getSQLValue (null, sql, getM_PriceList_ID());
			if (ii != 0)
				setC_Currency_ID (ii);
			else
				setC_Currency_ID(Env.getContextAsInt(getCtx(), "#C_Currency_ID"));
		}

		//	Sales Rep
		if (getSalesRep_ID() == 0)
		{
			int ii = Env.getContextAsInt(getCtx(), "#SalesRep_ID");
			if (ii != 0)
				setSalesRep_ID (ii);
		}

		//	Document Type
		if (getC_DocType_ID() == 0)
			setC_DocType_ID (0);	//	make sure it's set to 0
		if (getC_DocTypeTarget_ID() == 0)
			setC_DocTypeTarget_ID(isSOTrx() ? MDocType.DOCBASETYPE_ARInvoice : MDocType.DOCBASETYPE_APInvoice);

		//	Payment Term
		if (getC_PaymentTerm_ID() == 0)
		{
			int ii = Env.getContextAsInt(getCtx(), "#C_PaymentTerm_ID");
			if (ii != 0)
				setC_PaymentTerm_ID (ii);
			else
			{
				String sql = "SELECT C_PaymentTerm_ID FROM C_PaymentTerm WHERE AD_Client_ID=? AND IsDefault='Y'";
				ii = DB.getSQLValue(null, sql, getAD_Client_ID());
				if (ii != 0)
					setC_PaymentTerm_ID (ii);
			}
		}
		return true;
	}	//	beforeSave

	/**
	 * 	Before Delete
	 *	@return true if it can be deleted
	 */
	protected boolean beforeDelete ()
	{
		// Xpande. Gabriel Vila. 28/07/2017. Issue #1
		// Permito borrar invoices en Borrador sin importar que tenga orden asociada.
		// Comento codigo original.

		/*
		if (getC_Order_ID() != 0)
		{
			log.saveError("Error", Msg.getMsg(getCtx(), "CannotDelete"));
			return false;
		}
		*/

		// Fin Xpande.

		return true;
	}	//	beforeDelete

	/**
	 * 	String Representation
	 *	@return info
	 */
	public String toString ()
	{
		StringBuffer sb = new StringBuffer ("MInvoice[")
			.append(get_ID()).append("-").append(getDocumentNo())
			.append(",GrandTotal=").append(getGrandTotal());
		if (m_lines != null)
			sb.append(" (#").append(m_lines.length).append(")");
		sb.append ("]");
		return sb.toString ();
	}	//	toString

	/**
	 * 	Get Document Info
	 *	@return document info (untranslated)
	 */
	public String getDocumentInfo()
	{
		MDocType dt = MDocType.get(getCtx(), getC_DocType_ID());
		return dt.getName() + " " + getDocumentNo();
	}	//	getDocumentInfo


	/**
	 * 	After Save
	 *	@param newRecord new
	 *	@param success success
	 *	@return success
	 */
	protected boolean afterSave (boolean newRecord, boolean success)
	{
		if (!success || newRecord)
			return success;

		if (is_ValueChanged("AD_Org_ID"))
		{
			String sql = "UPDATE C_InvoiceLine ol"
				+ " SET AD_Org_ID ="
					+ "(SELECT AD_Org_ID"
					+ " FROM C_Invoice o WHERE ol.C_Invoice_ID=o.C_Invoice_ID) "
				+ "WHERE C_Invoice_ID=" + getC_Invoice_ID();
			int no = DB.executeUpdate(sql, get_TrxName());
			log.fine("Lines -> #" + no);
		}
		return true;
	}	//	afterSave


	/**
	 * 	Set Price List (and Currency) when valid
	 * 	@param M_PriceList_ID price list
	 */
	@Override
	public void setM_PriceList_ID (int M_PriceList_ID)
	{
		MPriceList pl = MPriceList.get(getCtx(), M_PriceList_ID, null);
		if (pl != null) {
			setC_Currency_ID(pl.getC_Currency_ID());
			super.setM_PriceList_ID(M_PriceList_ID);
		}
	}	//	setM_PriceList_ID


	/**
	 * 	Get Allocated Amt in Invoice Currency
	 *	@return pos/neg amount or null
	 */
	public BigDecimal getAllocatedAmt ()
	{
		BigDecimal retValue = null;
		String sql = "SELECT SUM(currencyConvert(al.Amount+al.DiscountAmt+al.WriteOffAmt,"
				+ "ah.C_Currency_ID, i.C_Currency_ID,ah.DateTrx,COALESCE(i.C_ConversionType_ID,0), al.AD_Client_ID,al.AD_Org_ID)) "
			+ "FROM C_AllocationLine al"
			+ " INNER JOIN C_AllocationHdr ah ON (al.C_AllocationHdr_ID=ah.C_AllocationHdr_ID)"
			+ " INNER JOIN C_Invoice i ON (al.C_Invoice_ID=i.C_Invoice_ID) "
			+ "WHERE al.C_Invoice_ID=?"
			+ " AND ah.IsActive='Y' AND al.IsActive='Y'";
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try
		{
			pstmt = DB.prepareStatement(sql, get_TrxName());
			pstmt.setInt(1, getC_Invoice_ID());
			rs = pstmt.executeQuery();
			if (rs.next())
			{
				retValue = rs.getBigDecimal(1);
			}
			rs.close();
			pstmt.close();
			pstmt = null;
		}
		catch (SQLException e)
		{
			throw new DBException(e, sql);
		}
		finally
		{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}
	//	log.fine("getAllocatedAmt - " + retValue);
		//	? ROUND(NVL(v_AllocatedAmt,0), 2);
		return retValue;
	}	//	getAllocatedAmt

	/**
	 * 	Test Allocation (and set paid flag)
	 *	@return true if updated
	 */
	public boolean testAllocation()
	{
		boolean change = false;

		if ( isProcessed() ) {
			BigDecimal alloc = getAllocatedAmt();	//	absolute
			if (alloc == null)
				alloc = Env.ZERO;
			BigDecimal total = getGrandTotal();
			if (!isSOTrx())
				total = total.negate();
			if (isCreditMemo())
				total = total.negate();
			boolean test = total.compareTo(alloc) == 0;
			change = test != isPaid();
			if (change)
				setIsPaid(test);
			log.fine("Paid=" + test
					+ " (" + alloc + "=" + total + ")");
		}

		return change;
	}	//	testAllocation

	/**
	 * 	Set Paid Flag for invoices
	 * 	@param ctx context
	 *	@param C_BPartner_ID if 0 all
	 *	@param trxName transaction
	 */
	public static void setIsPaid (Properties ctx, int C_BPartner_ID, String trxName)
	{
		List<Object> params = new ArrayList<Object>();
		StringBuffer whereClause = new StringBuffer("IsPaid='N' AND DocStatus IN ('CO','CL')");
		if (C_BPartner_ID > 1)
		{
			whereClause.append(" AND C_BPartner_ID=?");
			params.add(C_BPartner_ID);
		}
		else
		{
			whereClause.append(" AND AD_Client_ID=?");
			params.add(Env.getAD_Client_ID(ctx));
		}

		POResultSet<MInvoice> rs = new Query(ctx, MInvoice.Table_Name, whereClause.toString(), trxName)
										.setParameters(params)
										.scroll();
		int counter = 0;
		try {
			while(rs.hasNext()) {
				MInvoice invoice = rs.next();
				if (invoice.testAllocation())
					if (invoice.save())
						counter++;
			}
		}
		finally {
			DB.close(rs);
		}
		s_log.config("#" + counter);
		/**/
	}	//	setIsPaid

	/**
	 * 	Get Open Amount.
	 * 	Used by web interface
	 * 	@return Open Amt
	 */
	public BigDecimal getOpenAmt ()
	{
		return getOpenAmt (true, null);
	}	//	getOpenAmt

	/**
	 * 	Get Open Amount
	 * 	@param creditMemoAdjusted adjusted for CM (negative)
	 * 	@param paymentDate ignored Payment Date
	 * 	@return Open Amt
	 */
	public BigDecimal getOpenAmt (boolean creditMemoAdjusted, Timestamp paymentDate)
	{
		if (isPaid())
			return Env.ZERO;
		//
		if (m_openAmt == null)
		{
			m_openAmt = getGrandTotal();
			if (paymentDate != null)
			{
				//	Payment Discount
				//	Payment Schedule
			}
			BigDecimal allocated = getAllocatedAmt();
			if (allocated != null)
			{
				allocated = allocated.abs();	//	is absolute
				m_openAmt = m_openAmt.subtract(allocated);
			}
		}
		//
		if (!creditMemoAdjusted)
			return m_openAmt;
		if (isCreditMemo())
			return m_openAmt.negate();
		return m_openAmt;
	}	//	getOpenAmt


	/**
	 * 	Get Document Status
	 *	@return Document Status Clear Text
	 */
	public String getDocStatusName()
	{
		return MRefList.getListName(getCtx(), 131, getDocStatus());
	}	//	getDocStatusName


	/**************************************************************************
	 * 	Create PDF
	 *	@return File or null
	 */
	public File createPDF ()
	{
		try
		{
			File temp = File.createTempFile(get_TableName()+get_ID()+"_", ".pdf");
			return createPDF (temp);
		}
		catch (Exception e)
		{
			log.severe("Could not create PDF - " + e.getMessage());
		}
		return null;
	}	//	getPDF

	/**
	 * 	Create PDF file
	 *	@param file output file
	 *	@return file if success
	 */
	public File createPDF (File file)
	{
		ReportEngine re = ReportEngine.get (getCtx(), ReportEngine.INVOICE, getC_Invoice_ID(), get_TrxName());
		if (re == null)
			return null;
		return re.getPDF(file);
	}	//	createPDF

	/**
	 * 	Get PDF File Name
	 *	@param documentDir directory
	 *	@return file name
	 */
	public String getPDFFileName (String documentDir)
	{
		return getPDFFileName (documentDir, getC_Invoice_ID());
	}	//	getPDFFileName

	/**
	 *	Get ISO Code of Currency
	 *	@return Currency ISO
	 */
	public String getCurrencyISO()
	{
		return MCurrency.getISO_Code (getCtx(), getC_Currency_ID());
	}	//	getCurrencyISO

	/**
	 * 	Get Currency Precision
	 *	@return precision
	 */
	public int getPrecision()
	{
		return MCurrency.getStdPrecision(getCtx(), getC_Currency_ID());
	}	//	getPrecision


	/**************************************************************************
	 * 	Process document
	 *	@param processAction document action
	 *	@return true if performed
	 */
	public boolean processIt (String processAction)
	{
		m_processMsg = null;
		DocumentEngine engine = new DocumentEngine (this, getDocStatus());
		return engine.processIt (processAction, getDocAction());
	}	//	process

	/**	Process Message 			*/
	private String		m_processMsg = null;
	/**	Just Prepared Flag			*/
	private boolean		m_justPrepared = false;

	/**
	 * 	Unlock Document.
	 * 	@return true if success
	 */
	public boolean unlockIt()
	{
		log.info("unlockIt - " + toString());
		setProcessing(false);
		return true;
	}	//	unlockIt

	/**
	 * 	Invalidate Document
	 * 	@return true if success
	 */
	public boolean invalidateIt()
	{
		log.info("invalidateIt - " + toString());
		setDocAction(DOCACTION_Prepare);
		return true;
	}	//	invalidateIt

	/**
	 *	Prepare Document
	 * 	@return new status (In Progress or Invalid)
	 */
	public String prepareIt()
	{
		log.info(toString());
		m_processMsg = ModelValidationEngine.get().fireDocValidate(this, ModelValidator.TIMING_BEFORE_PREPARE);
		if (m_processMsg != null)
			return DocAction.STATUS_Invalid;

		MPeriod.testPeriodOpen(getCtx(), getDateAcct(), getC_DocTypeTarget_ID(), getAD_Org_ID());

		//	Lines
		MInvoiceLine[] lines = getLines(true);
		if (lines.length == 0)
		{
			m_processMsg = "@NoLines@";
			return DocAction.STATUS_Invalid;
		}
		//	No Cash Book
		if (PAYMENTRULE_Cash.equals(getPaymentRule())
			&& MCashBook.get(getCtx(), getAD_Org_ID(), getC_Currency_ID()) == null)
		{
			m_processMsg = "@NoCashBook@";
			return DocAction.STATUS_Invalid;
		}

		//	Convert/Check DocType
		if (getC_DocType_ID() != getC_DocTypeTarget_ID() )
			setC_DocType_ID(getC_DocTypeTarget_ID());
		if (getC_DocType_ID() == 0)
		{
			m_processMsg = "No Document Type";
			return DocAction.STATUS_Invalid;
		}

		explodeBOM();
		if (!calculateTaxTotal())	//	setTotals
		{
			m_processMsg = "Error calculating Tax";
			return DocAction.STATUS_Invalid;
		}

		createPaySchedule();

		//	Credit Status
		if (isSOTrx() && !isReversal())
		{
			MBPartner bp = new MBPartner (getCtx(), getC_BPartner_ID(), null);
			if (MBPartner.SOCREDITSTATUS_CreditStop.equals(bp.getSOCreditStatus()))
			{
				m_processMsg = "@BPartnerCreditStop@ - @TotalOpenBalance@="
					+ bp.getTotalOpenBalance()
					+ ", @SO_CreditLimit@=" + bp.getSO_CreditLimit();
				return DocAction.STATUS_Invalid;
			}
		}

		//	Landed Costs
		if (!isSOTrx())
		{
			for (int i = 0; i < lines.length; i++)
			{
				MInvoiceLine line = lines[i];
				String error = line.allocateLandedCosts();
				if (error != null && error.length() > 0)
				{
					m_processMsg = error;
					return DocAction.STATUS_Invalid;
				}
			}
		}

		m_processMsg = ModelValidationEngine.get().fireDocValidate(this, ModelValidator.TIMING_AFTER_PREPARE);
		if (m_processMsg != null)
			return DocAction.STATUS_Invalid;

		//	Add up Amounts
		m_justPrepared = true;
		if (!DOCACTION_Complete.equals(getDocAction()))
			setDocAction(DOCACTION_Complete);
		return DocAction.STATUS_InProgress;
	}	//	prepareIt

	/**
	 * 	Explode non stocked BOM.
	 */
	private void explodeBOM ()
	{
		String where = "AND IsActive='Y' AND EXISTS "
			+ "(SELECT * FROM M_Product p WHERE C_InvoiceLine.M_Product_ID=p.M_Product_ID"
			+ " AND	p.IsBOM='Y' AND p.IsVerified='Y' AND p.IsStocked='N')";
		//
		String sql = "SELECT COUNT(*) FROM C_InvoiceLine "
			+ "WHERE C_Invoice_ID=? " + where;
		int count = DB.getSQLValueEx(get_TrxName(), sql, getC_Invoice_ID());
		while (count != 0)
		{
			renumberLines (100);

			//	Order Lines with non-stocked BOMs
			MInvoiceLine[] lines = getLines (where);
			for (int i = 0; i < lines.length; i++)
			{
				MInvoiceLine line = lines[i];
				MProduct product = MProduct.get (getCtx(), line.getM_Product_ID());
				log.fine(product.getName());
				//	New Lines
				int lineNo = line.getLine ();

				//find default BOM with valid dates and to this product
				MPPProductBOM bom = MPPProductBOM.get(product, getAD_Org_ID(),getDateInvoiced(), get_TrxName());
				if(bom != null)
				{
					MPPProductBOMLine[] bomlines = bom.getLines(getDateInvoiced());
					for (int j = 0; j < bomlines.length; j++)
					{
						MPPProductBOMLine bomline = bomlines[j];
						MInvoiceLine newLine = new MInvoiceLine (this);
						newLine.setLine (++lineNo);
						newLine.setM_Product_ID (bomline.getM_Product_ID ());
						newLine.setC_UOM_ID (bomline.getC_UOM_ID ());
						newLine.setQty (line.getQtyInvoiced().multiply(
								bomline.getQtyBOM ()));		//	Invoiced/Entered
						if (bomline.getDescription () != null)
							newLine.setDescription (bomline.getDescription ());
						//
						newLine.setPrice ();
						newLine.saveEx (get_TrxName());
					}
				}

				/*MProductBOM[] boms = MProductBOM.getBOMLines (product);
				for (int j = 0; j < boms.length; j++)
				{
					MProductBOM bom = boms[j];
					MInvoiceLine newLine = new MInvoiceLine (this);
					newLine.setLine (++lineNo);
					newLine.setM_Product_ID (bom.getProduct().getM_Product_ID(),
						bom.getProduct().getC_UOM_ID());
					newLine.setQty (line.getQtyInvoiced().multiply(
						bom.getBOMQty ()));		//	Invoiced/Entered
					if (bom.getDescription () != null)
						newLine.setDescription (bom.getDescription ());
					//
					newLine.setPrice ();
					newLine.save (get_TrxName());
				}*/

				//	Convert into Comment Line
				line.setM_Product_ID (0);
				line.setM_AttributeSetInstance_ID (0);
				line.setPriceEntered (Env.ZERO);
				line.setPriceActual (Env.ZERO);
				line.setPriceLimit (Env.ZERO);
				line.setPriceList (Env.ZERO);
				line.setLineNetAmt (Env.ZERO);
				//
				String description = product.getName ();
				if (product.getDescription () != null)
					description += " " + product.getDescription ();
				if (line.getDescription () != null)
					description += " " + line.getDescription ();
				line.setDescription (description);
				line.saveEx (get_TrxName());
			} //	for all lines with BOM

			m_lines = null;
			count = DB.getSQLValue (get_TrxName(), sql, getC_Invoice_ID ());
			renumberLines (10);
		}	//	while count != 0
	}	//	explodeBOM

	/**
	 * 	Calculate Tax and Total
	 * 	@return true if calculated
	 */
	public boolean calculateTaxTotal()
	{
		log.fine("");
		//	Delete Taxes
		DB.executeUpdateEx("DELETE C_InvoiceTax WHERE C_Invoice_ID=" + getC_Invoice_ID(), get_TrxName());
		m_taxes = null;

		//	Lines
		BigDecimal totalLines = Env.ZERO;
		ArrayList<Integer> taxList = new ArrayList<Integer>();
		MInvoiceLine[] lines = getLines(false);
		for (int i = 0; i < lines.length; i++)
		{
			MInvoiceLine line = lines[i];
			if (!taxList.contains(line.getC_Tax_ID()))
			{
				MInvoiceTax iTax = MInvoiceTax.get (line, getPrecision(), false, get_TrxName()); //	current Tax
				if (iTax != null)
				{
					iTax.setIsTaxIncluded(isTaxIncluded());
					if (!iTax.calculateTaxFromLines())
						return false;
					iTax.saveEx();
					taxList.add(line.getC_Tax_ID());
				}
			}
			totalLines = totalLines.add(line.getLineNetAmt());
		}

		//	Taxes
		BigDecimal grandTotal = totalLines;
		MInvoiceTax[] taxes = getTaxes(true);
		for (int i = 0; i < taxes.length; i++)
		{
			MInvoiceTax iTax = taxes[i];
			MTax tax = iTax.getTax();
			if (tax.isSummary())
			{
				MTax[] cTaxes = tax.getChildTaxes(false);	//	Multiple taxes
				for (int j = 0; j < cTaxes.length; j++)
				{
					MTax cTax = cTaxes[j];
					BigDecimal taxAmt = cTax.calculateTax(iTax.getTaxBaseAmt(), isTaxIncluded(), getPrecision());
					//
					MInvoiceTax newITax = new MInvoiceTax(getCtx(), 0, get_TrxName());
					newITax.setClientOrg(this);
					newITax.setC_Invoice_ID(getC_Invoice_ID());
					newITax.setC_Tax_ID(cTax.getC_Tax_ID());
					newITax.setPrecision(getPrecision());
					newITax.setIsTaxIncluded(isTaxIncluded());
					newITax.setTaxBaseAmt(iTax.getTaxBaseAmt());
					newITax.setTaxAmt(taxAmt);
					newITax.saveEx(get_TrxName());
					//
					if (!isTaxIncluded())
						grandTotal = grandTotal.add(taxAmt);
				}
				iTax.deleteEx(true, get_TrxName());
			}
			else
			{
				if (!isTaxIncluded())
					grandTotal = grandTotal.add(iTax.getTaxAmt());
			}
		}
		//
		setTotalLines(totalLines);
		setGrandTotal(grandTotal);
		return true;
	}	//	calculateTaxTotal


	/**
	 * 	(Re) Create Pay Schedule
	 *	@return true if valid schedule
	 */
	private boolean createPaySchedule()
	{
		if (getC_PaymentTerm_ID() == 0)
			return false;
		MPaymentTerm pt = new MPaymentTerm(getCtx(), getC_PaymentTerm_ID(), null);
		log.fine(pt.toString());
		return pt.apply(this);		//	calls validate pay schedule
	}	//	createPaySchedule


	/**
	 * 	Approve Document
	 * 	@return true if success
	 */
	public boolean  approveIt()
	{
		log.info(toString());
		setIsApproved(true);
		return true;
	}	//	approveIt

	/**
	 * 	Reject Approval
	 * 	@return true if success
	 */
	public boolean rejectIt()
	{
		log.info(toString());
		setIsApproved(false);
		return true;
	}	//	rejectIt

	/**
	 * 	Complete Document
	 * 	@return new status (Complete, In Progress, Invalid, Waiting ..)
	 */
	public String completeIt()
	{
		//	Re-Check
		if (!m_justPrepared)
		{
			String status = prepareIt();
			if (!DocAction.STATUS_InProgress.equals(status))
				return status;
		}

		m_processMsg = ModelValidationEngine.get().fireDocValidate(this, ModelValidator.TIMING_BEFORE_COMPLETE);
		if (m_processMsg != null)
			return DocAction.STATUS_Invalid;

		//	Implicit Approval
		if (!isApproved())
			approveIt();
		log.info(toString());
		StringBuffer info = new StringBuffer();
		
		// POS supports multiple payments
		boolean fromPOS = false;
		if ( getC_Order_ID() > 0 )
		{
			fromPOS = getC_Order().getC_POS_ID() > 0;
		}

  		//	Create Cash
		if (PAYMENTRULE_Cash.equals(getPaymentRule()) && !fromPOS )
		{
			if (MSysConfig.getBooleanValue("CASH_AS_PAYMENT", true, getAD_Client_ID()))
			{
				String error = payCashWithCashAsPayment();
				if (error != "")
					return error;
			}
			
			MCash cash;

            int posId = Env.getContextAsInt(getCtx(),Env.POS_ID);

            if (posId != 0)
            {
                MPOS pos = new MPOS(getCtx(),posId,get_TrxName());
                int cashBookId = pos.getC_CashBook_ID();
                cash = MCash.get(getCtx(),cashBookId,getDateInvoiced(),get_TrxName());
            }
            else
            {
                cash = MCash.get (getCtx(), getAD_Org_ID(),
                        getDateInvoiced(), getC_Currency_ID(), get_TrxName());
            }

            // End Posterita Modifications

			if (cash == null || cash.get_ID() == 0)
			{
				m_processMsg = "@NoCashBook@";
				return DocAction.STATUS_Invalid;
			}
			MCashLine cl = new MCashLine (cash);
			cl.setInvoice(this);
			if (!cl.save(get_TrxName()))
			{
				m_processMsg = "Could not save Cash Journal Line";
				return DocAction.STATUS_Invalid;
			}
			info.append("@C_Cash_ID@: " + cash.getName() +  " #" + cl.getLine());
			setC_CashLine_ID(cl.getC_CashLine_ID());
		}	//	CashBook

		//	Update Order & Match
		AtomicInteger matchInvoices = new AtomicInteger(0);
		AtomicInteger matchOrders = new AtomicInteger(0);
		String docBaseType = getC_DocType().getDocBaseType();

		Arrays.stream(getLines(false))
				.filter(invoiceLine -> invoiceLine != null)
				.forEach( invoiceLine -> {
			//	Update Order Line
			MOrderLine orderLine = null;
			if (invoiceLine.getC_OrderLine_ID() != 0)
			{
				if (isSOTrx() || invoiceLine.getM_Product_ID() == 0)
				{
					orderLine = new MOrderLine (getCtx(), invoiceLine.getC_OrderLine_ID(), get_TrxName());
					//increase invoice quantity
					if ((isSOTrx() && MDocType.DOCBASETYPE_ARInvoice.equals(docBaseType)        && invoiceLine.getQtyInvoiced().signum() > 0)  // Quantity invoiced
					||	(isSOTrx() && MDocType.DOCBASETYPE_ARCreditMemo.equals(docBaseType)     && invoiceLine.getQtyInvoiced().signum() < 0)) // Revert AR Credit Memo
						orderLine.setQtyInvoiced(orderLine.getQtyInvoiced().add(invoiceLine.getQtyInvoiced().abs()));
					else if ((isSOTrx() && MDocType.DOCBASETYPE_ARInvoice.equals(docBaseType)   && invoiceLine.getQtyInvoiced().signum() < 0) // Revert Invoiced
					|| (isSOTrx() && MDocType.DOCBASETYPE_ARCreditMemo.equals(docBaseType)      && invoiceLine.getQtyInvoiced().signum() > 0)) // AR Credit Memo
						orderLine.setQtyInvoiced(orderLine.getQtyInvoiced().add(invoiceLine.getQtyInvoiced().abs().negate()));
					/*else if ((!isSOTrx() &&  MDocType.DOCBASETYPE_APInvoice.equals(docBaseType) && invoiceLine.getQtyInvoiced().signum() > 0) // Vendor Receipt
					|| (!isSOTrx() &&  MDocType.DOCBASETYPE_APCreditMemo.equals(docBaseType)    && invoiceLine.getQtyInvoiced().signum() < 0)) // Revert Return Vendor
						orderLine.setQtyInvoiced(orderLine.getQtyInvoiced().add(invoiceLine.getQtyInvoiced().abs());
					else if ((!isSOTrx() &&  MDocType.DOCBASETYPE_APInvoice.equals(docBaseType) && invoiceLine.getQtyInvoiced().signum() < 0)  // Revert Vendor Receipt
					|| (!isSOTrx() &&  MDocType.DOCBASETYPE_APCreditMemo.equals(docBaseType)    && invoiceLine.getQtyInvoiced().signum() > 0))  // Return Vendor
						orderLine.setQtyInvoiced(orderLine.getQtyInvoiced().add(invoiceLine.getQtyInvoiced().abs()));*/
					orderLine.saveEx();
					/*if (!orderLine.save(get_TrxName()))
					{
						m_processMsg = "Could not update Order Line";
						return DocAction.STATUS_Invalid;
					}*/
				}
				//	Order Invoiced Qty updated via Matching Inv-PO
				else if (!isSOTrx()
					&& invoiceLine.getM_Product_ID() != 0
					&& !isReversal())
				{
					//	MatchPO is created also from MInOut when Invoice exists before Shipment
					BigDecimal matchQty = invoiceLine.getQtyInvoiced();
					MMatchPO matchPO = MMatchPO.create (invoiceLine, null,
						getDateInvoiced(), matchQty);
					boolean isNewMatchPO = false;
					if (matchPO.get_ID() == 0)
						isNewMatchPO = true;
					matchPO.saveEx();
					/*if (!matchPO.save(get_TrxName()))
					{
						m_processMsg = "Could not create PO Matching";
						return DocAction.STATUS_Invalid;
					}*/
					
					matchOrders.getAndUpdate(record -> record + 1);
					if (isNewMatchPO)
						addDocsPostProcess(matchPO);
				}
			}

			//Update QtyInvoiced RMA Line
			if (invoiceLine.getM_RMALine_ID() != 0)
			{
				MRMALine rmaLine = new MRMALine (getCtx(),invoiceLine.getM_RMALine_ID(), get_TrxName());
				if (rmaLine.getQtyInvoiced() != null)
					rmaLine.setQtyInvoiced(rmaLine.getQtyInvoiced().add(invoiceLine.getQtyInvoiced()));
				else
					rmaLine.setQtyInvoiced(invoiceLine.getQtyInvoiced());

				rmaLine.saveEx();
				/*if (!rmaLine.save(get_TrxName()))
				{
					m_processMsg = "Could not update RMA Line";
					return DocAction.STATUS_Invalid;
				}*/
			}
			//

			//	Matching - Inv-Shipment
			if (!isSOTrx()
				&& invoiceLine.getM_InOutLine_ID() != 0
				&& invoiceLine.getM_Product_ID() != 0
				&& !isReversal())
			{
				MInOutLine receiptLine = new MInOutLine (getCtx(),invoiceLine.getM_InOutLine_ID(), get_TrxName());
				BigDecimal matchQty = invoiceLine.getQtyInvoiced();

				if (receiptLine.getMovementQty().compareTo(matchQty) < 0)
					matchQty = receiptLine.getMovementQty();

				MMatchInv matchInvoice = new MMatchInv(invoiceLine, getDateInvoiced(), matchQty);
				boolean isNewMatchInv = false;
				if (matchInvoice.get_ID() == 0)
					isNewMatchInv = true;
				matchInvoice.saveEx();
				/*if (!inv.save(get_TrxName()))
				{
					m_processMsg = CLogger.retrieveErrorString("Could not create Invoice Matching");
					return DocAction.STATUS_Invalid;
				}*/
				matchInvoices.getAndUpdate( record -> record + 1);
				if (isNewMatchInv)
					addDocsPostProcess(matchInvoice);
			}
		});	//	for all lines

		if (matchInvoices.get() > 0)
			info.append(" @M_MatchInv_ID@#").append(matchInvoices.get()).append(" ");
		if (matchOrders.get() > 0)
			info.append(" @M_MatchPO_ID@#").append(matchOrders.get()).append(" ");



		//	Update BP Statistics
		MBPartner bp = new MBPartner (getCtx(), getC_BPartner_ID(), get_TrxName());
		//	Update total revenue and balance / credit limit (reversed on AllocationLine.processIt)
		BigDecimal invAmt = MConversionRate.convertBase(getCtx(), getGrandTotal(true),	//	CM adjusted
			getC_Currency_ID(), getDateAcct(), getC_ConversionType_ID(), getAD_Client_ID(), getAD_Org_ID());
		if (invAmt == null)
		{
			m_processMsg = "Could not convert C_Currency_ID=" + getC_Currency_ID()
				+ " to base C_Currency_ID=" + MClient.get(Env.getCtx()).getC_Currency_ID();
			return DocAction.STATUS_Invalid;
		}
		//	Total Balance
		BigDecimal newBalance = bp.getTotalOpenBalance(false);
		if (newBalance == null)
			newBalance = Env.ZERO;
		if (isSOTrx())
		{
			newBalance = newBalance.add(invAmt);
			//
			if (bp.getFirstSale() == null)
				bp.setFirstSale(getDateInvoiced());
			BigDecimal newLifeAmt = bp.getActualLifeTimeValue();
			if (newLifeAmt == null)
				newLifeAmt = invAmt;
			else
				newLifeAmt = newLifeAmt.add(invAmt);
			BigDecimal newCreditAmt = bp.getSO_CreditUsed();
			if (newCreditAmt == null)
				newCreditAmt = invAmt;
			else
				newCreditAmt = newCreditAmt.add(invAmt);
			//
			log.fine("GrandTotal=" + getGrandTotal(true) + "(" + invAmt
				+ ") BP Life=" + bp.getActualLifeTimeValue() + "->" + newLifeAmt
				+ ", Credit=" + bp.getSO_CreditUsed() + "->" + newCreditAmt
				+ ", Balance=" + bp.getTotalOpenBalance(false) + " -> " + newBalance);
			bp.setActualLifeTimeValue(newLifeAmt);
			bp.setSO_CreditUsed(newCreditAmt);
		}	//	SO
		else
		{
			newBalance = newBalance.subtract(invAmt);
			log.fine("GrandTotal=" + getGrandTotal(true) + "(" + invAmt
				+ ") Balance=" + bp.getTotalOpenBalance(false) + " -> " + newBalance);
		}
		bp.setTotalOpenBalance(newBalance);
		bp.setSOCreditStatus();
		if (!bp.save(get_TrxName()))
		{
			m_processMsg = "Could not update Business Partner";
			return DocAction.STATUS_Invalid;
		}

		//	User - Last Result/Contact
		if (getAD_User_ID() != 0)
		{
			MUser user = new MUser (getCtx(), getAD_User_ID(), get_TrxName());
			user.setLastContact(new Timestamp(System.currentTimeMillis()));
			user.setLastResult(Msg.translate(getCtx(), "C_Invoice_ID") + ": " + getDocumentNo());
			if (!user.save(get_TrxName()))
			{
				m_processMsg = "Could not update Business Partner User";
				return DocAction.STATUS_Invalid;
			}
		}	//	user

		//	Update Project
		if (isSOTrx() && getC_Project_ID() != 0)
		{
			MProject project = new MProject (getCtx(), getC_Project_ID(), get_TrxName());
			BigDecimal amt = getGrandTotal(true);
			int C_CurrencyTo_ID = project.getC_Currency_ID();
			if (C_CurrencyTo_ID != getC_Currency_ID())
				amt = MConversionRate.convert(getCtx(), amt, getC_Currency_ID(), C_CurrencyTo_ID,
					getDateAcct(), 0, getAD_Client_ID(), getAD_Org_ID());
			if (amt == null)
			{
				m_processMsg = "Could not convert C_Currency_ID=" + getC_Currency_ID()
					+ " to Project C_Currency_ID=" + C_CurrencyTo_ID;
				return DocAction.STATUS_Invalid;
			}
			BigDecimal newAmt = project.getInvoicedAmt();
			if (newAmt == null)
				newAmt = amt;
			else
				newAmt = newAmt.add(amt);
			log.fine("GrandTotal=" + getGrandTotal(true) + "(" + amt
				+ ") Project " + project.getName()
				+ " - Invoiced=" + project.getInvoicedAmt() + "->" + newAmt);
			project.setInvoicedAmt(newAmt);
			if (!project.save(get_TrxName()))
			{
				m_processMsg = "Could not update Project";
				return DocAction.STATUS_Invalid;
			}
		}	//	project

		//	User Validation
		String valid = ModelValidationEngine.get().fireDocValidate(this, ModelValidator.TIMING_AFTER_COMPLETE);
		if (valid != null)
		{
			m_processMsg = valid;
			return DocAction.STATUS_Invalid;
		}

		// Set the definite document number after completed (if needed)
		setDefiniteDocumentNo();

		//	Counter Documents
		MInvoice counter = createCounterDoc();
		if (counter != null)
			info.append(" - @CounterDoc@: @C_Invoice_ID@=").append(counter.getDocumentNo());

		// Cfe
		if (docBaseType.equalsIgnoreCase(MDocType.DOCBASETYPE_ARInvoice)){
			this.cfe();
		}

		m_processMsg = info.toString().trim();
		setProcessed(true);
		setDocAction(DOCACTION_None);
		return DocAction.STATUS_Completed;
	}	//	completeIt

	/* Save array of documents to process AFTER completing this one */
	ArrayList<PO> docsPostProcess = new ArrayList<PO>();

	private void addDocsPostProcess(PO doc) {
		docsPostProcess.add(doc);
	}

	public ArrayList<PO> getDocsPostProcess() {
		return docsPostProcess;
	}

	/**
	 * 	Set the definite document number after completed
	 */
	private void setDefiniteDocumentNo() {
		MDocType dt = MDocType.get(getCtx(), getC_DocType_ID());
		if (dt.isOverwriteDateOnComplete()) {
			setDateInvoiced(new Timestamp (System.currentTimeMillis()));
		}
		if (dt.isOverwriteSeqOnComplete()) {
			String value = DB.getDocumentNo(getC_DocType_ID(), get_TrxName(), true, this);
			if (value != null)
				setDocumentNo(value);
		}
	}

	/**
	 * 	Create Counter Document
	 * 	@return counter invoice
	 */
	private MInvoice createCounterDoc()
	{
		//	Is this a counter doc ?
		if (getRef_Invoice_ID() != 0)
			return null;

		//	Org Must be linked to BPartner
		MOrg org = MOrg.get(getCtx(), getAD_Org_ID());
		int counterC_BPartner_ID = org.getLinkedC_BPartner_ID(get_TrxName());
		if (counterC_BPartner_ID == 0)
			return null;
		//	Business Partner needs to be linked to Org
		MBPartner bp = new MBPartner (getCtx(), getC_BPartner_ID(), get_TrxName());
		int counterAD_Org_ID = bp.getAD_OrgBP_ID_Int();
		if (counterAD_Org_ID == 0)
			return null;

		MBPartner counterBP = new MBPartner (getCtx(), counterC_BPartner_ID, get_TrxName());
//		MOrgInfo counterOrgInfo = MOrgInfo.get(getCtx(), counterAD_Org_ID);
		log.info("Counter BP=" + counterBP.getName());

		//	Document Type
		int C_DocTypeTarget_ID = 0;
		MDocTypeCounter counterDT = MDocTypeCounter.getCounterDocType(getCtx(), getC_DocType_ID());
		if (counterDT != null)
		{
			log.fine(counterDT.toString());
			if (!counterDT.isCreateCounter() || !counterDT.isValid())
				return null;
			C_DocTypeTarget_ID = counterDT.getCounter_C_DocType_ID();
		}
		else	//	indirect
		{
			C_DocTypeTarget_ID = MDocTypeCounter.getCounterDocType_ID(getCtx(), getC_DocType_ID());
			log.fine("Indirect C_DocTypeTarget_ID=" + C_DocTypeTarget_ID);
			if (C_DocTypeTarget_ID <= 0)
				return null;
		}

		//	Deep Copy
		MInvoice counter = copyFrom(this, getDateInvoiced(), getDateAcct(),
			C_DocTypeTarget_ID, !isSOTrx(), true, false, get_TrxName(), true);
		//
		counter.setAD_Org_ID(counterAD_Org_ID);
	//	counter.setM_Warehouse_ID(counterOrgInfo.getM_Warehouse_ID());
		//
		counter.setBPartner(counterBP);
		//	Refernces (Should not be required
		counter.setSalesRep_ID(getSalesRep_ID());
		counter.save(get_TrxName());

		//	Update copied lines
		MInvoiceLine[] counterLines = counter.getLines(true);
		for (int i = 0; i < counterLines.length; i++)
		{
			MInvoiceLine counterLine = counterLines[i];
			counterLine.setClientOrg(counter);
			counterLine.setInvoice(counter);	//	copies header values (BP, etc.)
			counterLine.setPrice();
			counterLine.setTax();
			//
			counterLine.save(get_TrxName());
		}

		log.fine(counter.toString());

		//	Document Action
		if (counterDT != null)
		{
			if (counterDT.getDocAction() != null)
			{
				counter.setDocAction(counterDT.getDocAction());
				counter.processIt(counterDT.getDocAction());
				counter.save(get_TrxName());
			}
		}
		return counter;
	}	//	createCounterDoc

	/**
	 * 	Void Document.
	 * 	@return true if success
	 */
	public boolean voidIt()
	{
		log.info(toString());
		// Before Void
		m_processMsg = ModelValidationEngine.get().fireDocValidate(this,ModelValidator.TIMING_BEFORE_VOID);
		if (m_processMsg != null)
			return false;

		if (DOCSTATUS_Closed.equals(getDocStatus())
			|| DOCSTATUS_Reversed.equals(getDocStatus())
			|| DOCSTATUS_Voided.equals(getDocStatus()))
		{
			m_processMsg = "Document Closed: " + getDocStatus();
			setDocAction(DOCACTION_None);
			return false;
		}

		//	Not Processed
		if (DOCSTATUS_Drafted.equals(getDocStatus())
			|| DOCSTATUS_Invalid.equals(getDocStatus())
			|| DOCSTATUS_InProgress.equals(getDocStatus())
			|| DOCSTATUS_Approved.equals(getDocStatus())
			|| DOCSTATUS_NotApproved.equals(getDocStatus()) )
		{
			//	Set lines to 0
			MInvoiceLine[] lines = getLines(false);
			for (int i = 0; i < lines.length; i++)
			{
				MInvoiceLine line = lines[i];
				BigDecimal old = line.getQtyInvoiced();
				if (old.compareTo(Env.ZERO) != 0)
				{
					line.setQty(Env.ZERO);
					line.setTaxAmt(Env.ZERO);
					line.setLineNetAmt(Env.ZERO);
					line.setLineTotalAmt(Env.ZERO);
					line.addDescription(Msg.getMsg(getCtx(), "Voided") + " (" + old + ")");
					//	Unlink Shipment
					if (line.getM_InOutLine_ID() != 0)
					{
						MInOutLine ioLine = new MInOutLine(getCtx(), line.getM_InOutLine_ID(), get_TrxName());
						ioLine.setIsInvoiced(false);
						ioLine.save(get_TrxName());
						line.setM_InOutLine_ID(0);
					}
					line.save(get_TrxName());
				}
			}
			addDescription(Msg.getMsg(getCtx(), "Voided"));
			setIsPaid(true);
			setC_Payment_ID(0);
		}
		else
		{
			return reverseCorrectIt();
		}

		// After Void
		m_processMsg = ModelValidationEngine.get().fireDocValidate(this,ModelValidator.TIMING_AFTER_VOID);
		if (m_processMsg != null)
			return false;

		setProcessed(true);
		setDocAction(DOCACTION_None);
		return true;
	}	//	voidIt

	/**
	 * 	Close Document.
	 * 	@return true if success
	 */
	public boolean closeIt()
	{
		log.info(toString());
		// Before Close
		m_processMsg = ModelValidationEngine.get().fireDocValidate(this,ModelValidator.TIMING_BEFORE_CLOSE);
		if (m_processMsg != null)
			return false;

		setProcessed(true);
		setDocAction(DOCACTION_None);

		// After Close
		m_processMsg = ModelValidationEngine.get().fireDocValidate(this,ModelValidator.TIMING_AFTER_CLOSE);
		if (m_processMsg != null)
			return false;
		return true;
	}	//	closeIt

	/**
	 * 	Reverse Correction - same date
	 * 	@return true if success
	 */
	public boolean reverseCorrectIt()
	{
		log.info(toString());
		// Before reverseCorrect
		m_processMsg = ModelValidationEngine.get().fireDocValidate(this,ModelValidator.TIMING_BEFORE_REVERSECORRECT);
		if (m_processMsg != null)
			return false;

		MPeriod.testPeriodOpen(getCtx(), getDateAcct(), getC_DocType_ID(), getAD_Org_ID());
		//
		MAllocationHdr[] allocations = MAllocationHdr.getOfInvoice(getCtx(),
			getC_Invoice_ID(), get_TrxName());
		for (int i = 0; i < allocations.length; i++)
		{
			allocations[i].setDocAction(DocAction.ACTION_Reverse_Correct);
			allocations[i].reverseCorrectIt();
			allocations[i].save(get_TrxName());
		}
		//	Reverse/Delete Matching
		if (!isSOTrx())
		{
			MMatchInv[] mInv = MMatchInv.getInvoice(getCtx(), getC_Invoice_ID(), get_TrxName());
			for (int i = 0; i < mInv.length; i++)
				mInv[i].delete(true);
			MMatchPO[] mPO = MMatchPO.getInvoice(getCtx(), getC_Invoice_ID(), get_TrxName());
			for (int i = 0; i < mPO.length; i++)
			{
				if (mPO[i].getM_InOutLine_ID() == 0)
					mPO[i].delete(true);
				else
				{
					mPO[i].setC_InvoiceLine_ID(null);
					mPO[i].save(get_TrxName());
				}
			}
		}
		//
		load(get_TrxName());	//	reload allocation reversal info

		//	Deep Copy
		MInvoice reversal = copyFrom (this, getDateInvoiced(), getDateAcct(),
			getC_DocType_ID(), isSOTrx(), false, true, get_TrxName(), true);
		if (reversal == null)
		{
			m_processMsg = "Could not create Invoice Reversal";
			return false;
		}
		//	Reverse Line Qty
		MInvoiceLine[] rLines = reversal.getLines(false);
		for (int i = 0; i < rLines.length; i++)
		{
			MInvoiceLine rLine = rLines[i];
			rLine.setQtyEntered(rLine.getQtyEntered().negate());
			rLine.setQtyInvoiced(rLine.getQtyInvoiced().negate());
			rLine.setLineNetAmt(rLine.getLineNetAmt().negate());
			if (rLine.getTaxAmt() != null && rLine.getTaxAmt().compareTo(Env.ZERO) != 0)
				rLine.setTaxAmt(rLine.getTaxAmt().negate());
			if (rLine.getLineTotalAmt() != null && rLine.getLineTotalAmt().compareTo(Env.ZERO) != 0)
				rLine.setLineTotalAmt(rLine.getLineTotalAmt().negate());
			if (!rLine.save(get_TrxName()))
			{
				m_processMsg = "Could not correct Invoice Reversal Line";
				return false;
			}
		}
		reversal.setC_Order_ID(getC_Order_ID());
		reversal.addDescription("{->" + getDocumentNo() + ")");
		//FR1948157
		reversal.saveEx(get_TrxName());
		//
		if (!reversal.processIt(DocAction.ACTION_Complete))
		{
			m_processMsg = "Reversal ERROR: " + reversal.getProcessMsg();
			return false;
		}
		reversal.setC_Payment_ID(0);
		reversal.setIsPaid(true);
		reversal.closeIt();
		reversal.setProcessing (false);
		reversal.setDocStatus(DOCSTATUS_Reversed);
		reversal.setDocAction(DOCACTION_None);
		reversal.saveEx(get_TrxName());
		m_processMsg = reversal.getDocumentNo();
		//
		addDescription("(" + reversal.getDocumentNo() + "<-)");

		//	Clean up Reversed (this)
		MInvoiceLine[] iLines = getLines(false);
		for (int i = 0; i < iLines.length; i++)
		{
			MInvoiceLine iLine = iLines[i];
			if (iLine.getM_InOutLine_ID() != 0)
			{
				MInOutLine ioLine = new MInOutLine(getCtx(), iLine.getM_InOutLine_ID(), get_TrxName());
				ioLine.setIsInvoiced(false);
				ioLine.save(get_TrxName());
				//	Reconsiliation
				iLine.setM_InOutLine_ID(0);
				iLine.save(get_TrxName());
			}
        }
		setProcessed(true);
		//FR1948157
		setReversal_ID(reversal.getC_Invoice_ID());
		setDocStatus(DOCSTATUS_Reversed);	//	may come from void
		setDocAction(DOCACTION_None);
		setC_Payment_ID(0);
		setIsPaid(true);

		//	Create Allocation
		MAllocationHdr alloc = new MAllocationHdr(getCtx(), false, getDateAcct(),
			getC_Currency_ID(),
			Msg.translate(getCtx(), "C_Invoice_ID")	+ ": " + getDocumentNo() + "/" + reversal.getDocumentNo(),
			get_TrxName());
		alloc.setAD_Org_ID(getAD_Org_ID());
		if (alloc.save())
		{
			//	Amount
			BigDecimal gt = getGrandTotal(true);
			if (!isSOTrx())
				gt = gt.negate();
			//	Orig Line
			MAllocationLine aLine = new MAllocationLine (alloc, gt,
				Env.ZERO, Env.ZERO, Env.ZERO);
			aLine.setC_Invoice_ID(getC_Invoice_ID());
			aLine.saveEx();
			//	Reversal Line
			MAllocationLine rLine = new MAllocationLine (alloc, gt.negate(),
				Env.ZERO, Env.ZERO, Env.ZERO);
			rLine.setC_Invoice_ID(reversal.getC_Invoice_ID());
			rLine.saveEx();
			//	Process It
			if (alloc.processIt(DocAction.ACTION_Complete))
				alloc.saveEx();
		}

		// After reverseCorrect
		m_processMsg = ModelValidationEngine.get().fireDocValidate(this,ModelValidator.TIMING_AFTER_REVERSECORRECT);
		if (m_processMsg != null)
			return false;

		return true;
	}	//	reverseCorrectIt

	/**
	 * 	Reverse Accrual - none
	 * 	@return false
	 */
	public boolean reverseAccrualIt()
	{
		log.info(toString());
		// Before reverseAccrual
		m_processMsg = ModelValidationEngine.get().fireDocValidate(this,ModelValidator.TIMING_BEFORE_REVERSEACCRUAL);
		if (m_processMsg != null)
			return false;

		// After reverseAccrual
		m_processMsg = ModelValidationEngine.get().fireDocValidate(this,ModelValidator.TIMING_AFTER_REVERSEACCRUAL);
		if (m_processMsg != null)
			return false;

		return false;
	}	//	reverseAccrualIt

	/**
	 * 	Re-activate
	 * 	@return false
	 */
	public boolean reActivateIt()
	{
		log.info(toString());
		// Before reActivate
		m_processMsg = ModelValidationEngine.get().fireDocValidate(this,ModelValidator.TIMING_BEFORE_REACTIVATE);
		if (m_processMsg != null)
			return false;

		// After reActivate
		m_processMsg = ModelValidationEngine.get().fireDocValidate(this,ModelValidator.TIMING_AFTER_REACTIVATE);
		if (m_processMsg != null)
			return false;


		// Xpande. Gabriel Vila. 03/08/2017. Issue #2.
		// Me aseguro estados de documento al reactivar
		this.setProcessed(false);
		this.setPosted(false);
		this.setDocStatus(DOCSTATUS_InProgress);
		this.setDocAction(DOCACTION_Complete);

		// Comento retorno en false y por defecto que retorne true.
		// return false;
		return true;
		// Xpande

	}	//	reActivateIt


	/*************************************************************************
	 * 	Get Summary
	 *	@return Summary of Document
	 */
	public String getSummary()
	{
		StringBuffer sb = new StringBuffer();
		sb.append(getDocumentNo());
		//	: Grand Total = 123.00 (#1)
		sb.append(": ").
			append(Msg.translate(getCtx(),"GrandTotal")).append("=").append(getGrandTotal())
			.append(" (#").append(getLines(false).length).append(")");
		//	 - Description
		if (getDescription() != null && getDescription().length() > 0)
			sb.append(" - ").append(getDescription());
		return sb.toString();
	}	//	getSummary

	/**
	 * 	Get Process Message
	 *	@return clear text error message
	 */
	public String getProcessMsg()
	{
		return m_processMsg;
	}	//	getProcessMsg

	/**
	 * 	Get Document Owner (Responsible)
	 *	@return AD_User_ID
	 */
	public int getDoc_User_ID()
	{
		return getSalesRep_ID();
	}	//	getDoc_User_ID

	/**
	 * 	Get Document Approval Amount
	 *	@return amount
	 */
	public BigDecimal getApprovalAmt()
	{
		return getGrandTotal();
	}	//	getApprovalAmt

	/**
	 *
	 * @param rma
	 */
	public void setRMA(MRMA rma)
	{
		setM_RMA_ID(rma.getM_RMA_ID());
        setAD_Org_ID(rma.getAD_Org_ID());
        setDescription(rma.getDescription());
        setC_BPartner_ID(rma.getC_BPartner_ID());
        setSalesRep_ID(rma.getSalesRep_ID());

        setGrandTotal(rma.getAmt());
        setIsSOTrx(rma.isSOTrx());
        setTotalLines(rma.getAmt());

        MInvoice originalInvoice = rma.getOriginalInvoice();

        if (originalInvoice == null)
        {
            throw new IllegalStateException("Not invoiced - RMA: " + rma.getDocumentNo());
        }

        setC_BPartner_Location_ID(originalInvoice.getC_BPartner_Location_ID());
        setAD_User_ID(originalInvoice.getAD_User_ID());
        setC_Currency_ID(originalInvoice.getC_Currency_ID());
        setIsTaxIncluded(originalInvoice.isTaxIncluded());
        setM_PriceList_ID(originalInvoice.getM_PriceList_ID());
        setC_Project_ID(originalInvoice.getC_Project_ID());
        setC_Activity_ID(originalInvoice.getC_Activity_ID());
        setC_Campaign_ID(originalInvoice.getC_Campaign_ID());
        setUser1_ID(originalInvoice.getUser1_ID());
        setUser2_ID(originalInvoice.getUser2_ID());
		setUser3_ID(originalInvoice.getUser3_ID());
		setUser4_ID(originalInvoice.getUser4_ID());
	}

	/**
	 * 	Document Status is Complete or Closed
	 *	@return true if CO, CL or RE
	 */
	public boolean isComplete()
	{
		String ds = getDocStatus();
		return DOCSTATUS_Completed.equals(ds)
			|| DOCSTATUS_Closed.equals(ds)
			|| DOCSTATUS_Reversed.equals(ds);
	}	//	isComplete
	
	/**
	 * Pay it with cash
	 * @return
	 */
	private String payCashWithCashAsPayment() {
		int posId = Env.getContextAsInt(getCtx(),Env.POS_ID);
		if(posId == 0) {
			return "@C_POS_ID@ @NotFound@";
		}
		//	
		MPOS pos = MPOS.get(getCtx(), posId);
		MPayment paymentCash = new MPayment(getCtx(), 0 ,  get_TrxName());
		paymentCash.setC_BankAccount_ID(pos.getC_BankAccount_ID());
		paymentCash.setC_DocType_ID(true);
        String value = DB.getDocumentNo(paymentCash.getC_DocType_ID(),get_TrxName(), false,  paymentCash);
        paymentCash.setDocumentNo(value);
        paymentCash.setDateAcct(getDateAcct());
        paymentCash.setDateTrx(getDateInvoiced());
        paymentCash.setTenderType(MPayment.TENDERTYPE_Cash);
        paymentCash.setDescription(getDescription());
        paymentCash.setC_BPartner_ID (getC_BPartner_ID());
        paymentCash.setC_Currency_ID(getC_Currency_ID());
        paymentCash.setPayAmt(getGrandTotal());
        paymentCash.setOverUnderAmt(Env.ZERO);
        paymentCash.setC_Invoice_ID(getC_Invoice_ID());
		paymentCash.saveEx();
		if (!paymentCash.processIt(X_C_Payment.DOCACTION_Complete))
			return DOCSTATUS_Invalid;
		paymentCash.saveEx();
		MBankStatement.addPayment(paymentCash);
		return "";
	}



	// Xpande. CFE
	private void cfe(){
		try{



			CFEEmpresasType objECfe = new CFEEmpresasType();
			CFEDefType objCfe = new CFEDefType();
			objECfe.setCFE(objCfe);

			//objCfe.setEFact(new CFEDefType.EFact());

			String cfeType = "141";

			loadEncabezado_eTicket_eFactura(cfeType, objCfe);
			loadDetalleProductosOServicios_eTicket_eFactura(cfeType, objCfe);
			//loadInformacionDeDescuentosORecargos_eTicket_eFactura(cfeType, objCfe);

			//loadInfoReferencia_eTicket_eFactura(cfeType, objCfe);

			//loadComplementoFiscal(cfeType, objCfe);
			loadCAE(objECfe);

			//loadTimestamp(cfeType, objCfe);
			objCfe.getEFact().setTmstFirma(Timestamp_to_XmlGregorianCalendar_OnlyDate(this.getDateInvoiced(), true));

			objECfe.setAdenda(this.getDescription());

			// Sigo
			if (objCfe.getEResg().getEncabezado() != null) {
				objCfe.getEResg().getEncabezado().setEmisor(null);
			}
			objCfe.getEResg().setTmstFirma(null);

			// Sigooo
			this.SendCfe(objCfe);



		}
		catch (Exception e){
		    throw new AdempiereException(e);
		}
	}


	private void loadEncabezado_eTicket_eFactura(String cfeType, CFEDefType objCfe) {

		String sql = "";
		ResultSet rs = null;
		PreparedStatement pstmt = null;

		CFEDefType.ETck eTicket = new CFEDefType.ETck();
		CFEDefType.EFact eFactura = new CFEDefType.EFact();
		CFEDefType.ETck.Encabezado eTicketEncabezado = new CFEDefType.ETck.Encabezado();
		CFEDefType.EFact.Encabezado eFactEncabezado = new CFEDefType.EFact.Encabezado();
		IdDocTck idDocTck = new IdDocTck();
		IdDocFact idDocFact = new IdDocFact();
		Emisor emisor = new Emisor();
		ReceptorTck receptorTck = new ReceptorTck();
		ReceptorFact receptorFact = new ReceptorFact();
		Totales totales = new Totales();

		/*
		if (cfeType == CfeType.eTicket || cfeType == CfeType.eTicket_ND || cfeType == CfeType.eTicket_NC
				|| cfeType == CfeType.eTicket_VxCA || cfeType == CfeType.eTicket_ND_VxCA || cfeType == CfeType.eTicket_NC_VxCA){
			objCfe.setETck(eTicket);
			eTicket.setEncabezado(eTicketEncabezado);
			eTicketEncabezado.setIdDoc(idDocTck);
			eTicketEncabezado.setEmisor(emisor);
			eTicketEncabezado.setReceptor(receptorTck);
			eTicketEncabezado.setTotales(totales);
		} else if (cfeType == CfeType.eFactura || cfeType == CfeType.eFactura_ND || cfeType == CfeType.eFactura_NC
				|| cfeType == CfeType.eFactura_VxCA || cfeType == CfeType.eFactura_ND_VxCA || cfeType == CfeType.eFactura_NC_VxCA){
			objCfe.setEFact(eFactura);
			eFactura.setEncabezado(eFactEncabezado);
			eFactEncabezado.setIdDoc(idDocFact);
			eFactEncabezado.setEmisor(emisor);
			eFactEncabezado.setReceptor(receptorFact);
			eFactEncabezado.setTotales(totales);
		}
		*/
		objCfe.setEFact(eFactura);
		eFactura.setEncabezado(eFactEncabezado);
		eFactEncabezado.setIdDoc(idDocFact);
		eFactEncabezado.setEmisor(emisor);
		eFactEncabezado.setReceptor(receptorFact);
		eFactEncabezado.setTotales(totales);

		objCfe.setVersion("1.0");

		//  AREA: Identificacion del Comprobante

		MDocType doc = MDocType.get(getCtx(), this.getC_DocTypeTarget_ID());
		//BigInteger doctype = new BigInteger(doc.get_Value("CfeType").toString());
		BigInteger doctype = new BigInteger("141");
		/* 2   */ idDocTck.setTipoCFE(doctype);
		/* 2   */ idDocFact.setTipoCFE(doctype);

		MSequence sec = new MSequence(getCtx(), doc.getDefiniteSequence_ID(), get_TrxName());
		if(sec.getPrefix() != null){
			/* 3   */ idDocTck.setSerie(sec.getPrefix());
			/* 3   */ idDocFact.setSerie(sec.getPrefix());
		} else {
			throw new AdempiereException("CFEMessages.IDDOC_003");
		}

		if(this.getDocumentNo() != null){ // Se obtiene nro de cae directamente del documentNo
			// Se quita serie del n�mero para enviar
			String documentNo = this.getDocumentNo();
			documentNo = documentNo.replaceAll("[^0-9]", ""); // Expresi�n regular para quitar todo lo que no es n�mero

			String docno = org.apache.commons.lang.StringUtils.leftPad(String.valueOf(documentNo), 7, "0");
			BigInteger numero = new BigInteger(docno);
			/* 4   */ idDocTck.setNro(numero);// N�mero de CFE 7 digitos
			/* 4   */ idDocFact.setNro(numero);
		}
		else throw new AdempiereException("CFEMessages.IDDOC_004");

		if (this.getDateInvoiced() != null){
			/* 5   */ idDocTck.setFchEmis(Timestamp_to_XmlGregorianCalendar_OnlyDate(this.getDateInvoiced(), false));
			/* 5   */ idDocFact.setFchEmis(Timestamp_to_XmlGregorianCalendar_OnlyDate(this.getDateInvoiced(), false));

		} else {
			throw new AdempiereException("CFEMessages.IDDOC_005");
		}
		/* 6    - No Corresponde */
		/* 7   - Tipo de obligatoriedad 3 (dato opcional)*/ //idDocTck.setPeriodoDesde(null);
		/* 7   - Tipo de obligatoriedad 3 (dato opcional)*/ //idDocFact.setPeriodoDesde(null);
		/* 8   - Tipo de obligatoriedad 3 (dato opcional)*/ //idDocTck.setPeriodoHasta(null);
		/* 8   - Tipo de obligatoriedad 3 (dato opcional)*/ //idDocFact.setPeriodoHasta(null);


		/*
		 *  OpenUp Ltda. - #5821 - Raul Capecce
		 *  Si la lista de precios tiene los impuestos incluidos,
		 *	se indica en el cfe que las lineas van con los montos brutos en vez de netos
		 */
		/*
		 *  OpenUp Ltda. - #7550 - Raul Capecce - 18/10/2016
		 *  Se determina que el CFE se va a mandar con los impuestos incluidos a nivel de montos de la linea
		 *  A nivel de linea se va a calcular para que los totales sean con impuestos incluidos
		 */
		/*
		 *  OpenUp Ltda. - #7610 - Raul Capecce - 25/10/2016
		 *  Nicolas Lopez y Gabriel Vila indican que se deben mandar los totales como aparecen en Adempiere
		 *  En el caso que corresponda con o sin impuestos incluidos
		 */
		MPriceList priceList = (MPriceList) this.getM_PriceList();
		if (priceList != null && priceList.get_ValueAsBoolean("isTaxIncluded")) {
			/* 10  */ idDocTck.setMntBruto(new BigInteger("1"));
			/* 10  */ idDocFact.setMntBruto(new BigInteger("1"));
		} else {
			/* 10  */ idDocTck.setMntBruto(new BigInteger("0"));
			/* 10  */ idDocFact.setMntBruto(new BigInteger("0"));
		}
//		/* 10  */ idDocTck.setMntBruto(new BigInteger("1"));
//		/* 10  */ idDocFact.setMntBruto(new BigInteger("1"));
		/*  OpenUp Ltda. - #7610 - Fin */
		/*  OpenUp Ltda. - #7550 - Fin */
		/*  OpenUp Ltda. - #5821 - Fin */

		//
		//if(mInvoice.getpaymentruletype().equalsIgnoreCase("CO")){
			/* 11  */ idDocTck.setFmaPago(BigInteger.valueOf(1));
			/* 11  */ idDocFact.setFmaPago(BigInteger.valueOf(1));
		//} else if(mInvoice.getpaymentruletype().equalsIgnoreCase("CR")){
			/* 11  */ idDocTck.setFmaPago(BigInteger.valueOf(2));
			/* 11  */ idDocFact.setFmaPago(BigInteger.valueOf(2));
		//} else {
		//	throw new AdempiereException(CFEMessages.IDDOC_011);
		//}


		/* 11  */ idDocTck.setFmaPago(BigInteger.valueOf(2));
		/* 11  */ idDocFact.setFmaPago(BigInteger.valueOf(2));


		//if (mInvoice.getDueDate() != null) {
		//	/* 12  */ idDocTck.setFchVenc(CfeUtils.Timestamp_to_XmlGregorianCalendar_OnlyDate(mInvoice.getDueDate(), false));
		//	/* 12  */ idDocFact.setFchVenc(CfeUtils.Timestamp_to_XmlGregorianCalendar_OnlyDate(mInvoice.getDueDate(), false));
		//}


		/* 13   - No Corresponde */
		/* 14   - No Corresponde */
		/* 15   - No Corresponde */


		//  AREA: Emisor

		//MOrgInfo orgInfo = MOrgInfo.get(ctx, mInvoice.getAD_Org_ID(), trxName);
		//if (orgInfo == null) throw new AdempiereException(CFEMessages.EMISOR_ORG);

		//if (orgInfo.getDUNS() == null) throw new AdempiereException(CFEMessages.EMISOR_040);
		/* 40  */ emisor.setRUCEmisor("212334750012");

		//if (orgInfo.getrznsoc() == null) throw new AdempiereException(CFEMessages.EMISOR_041);
		/* 41  */ emisor.setRznSoc("212334750012");
		//MOrg mOrg = MOrg.get(ctx, orgInfo.getAD_Org_ID());

		// OpenUp Ltda. - #6853 - Raul Capecce - 19/09/2016
		// Campo no obligatorio, seg�n petici�n, no se env�a en el xml
		// if (mOrg != null && mOrg.getName() != null) {
		// 	/* 42  */ emisor.setNomComercial(MOrg.get(ctx, orgInfo.getAD_Org_ID()).getName());
		// }
		// FIN - #6853


		///* 43  */ emisor.setGiroEmis(orgInfo.getgirotype());
		//emisor.setNomComercial("Supermercado Covadonga S.A.");
		//emisor.setEmiSucursal("Covadonga");

		// OpenUp Ltda. - #6853 - Ra�l Capeccce - Dato requerido por el cliente
		//if (orgInfo.getPhone() != null) {
			///* 44  */ emisor.getTelefono().add(orgInfo.getPhone());
//		}
		// FIN - #6853

		///* 45  */ emisor.setCorreoEmisor(orgInfo.getEMail());
//		MWarehouse casa = MWarehouse.get(ctx, orgInfo.getDropShip_Warehouse_ID());
//		/* 46  */ emisor.setEmiSucursal(casa.getName());
		try {
			/* 47  */ emisor.setCdgDGISucur(BigInteger.valueOf(1));
		}catch(Exception ex){
			throw new AdempiereException("CFEMessages.EMISOR_047");
		}


		//MLocation mLocation = (MLocation) orgInfo.getC_Location();
		//if (mLocation == null || mLocation.getAddress1() == null) throw new AdempiereException(CFEMessages.EMISOR_048);
		/* 48  */ emisor.setDomFiscal("Progreso");
		//MLocalidades mLocalidades = (MLocalidades) mLocation.getUY_Localidades();
		//if (mLocalidades == null || mLocalidades.getName() == null) throw new AdempiereException(CFEMessages.EMISOR_049);
		/* 49  */ emisor.setCiudad("PROGRESO");
		//MDepartamentos mDepartamentos = (MDepartamentos) mLocation.getUY_Departamentos();
		//if (mDepartamentos == null || mDepartamentos.getName() == null) throw new AdempiereException(CFEMessages.EMISOR_050);
		/* 50  */ emisor.setDepartamento("CANELONES");


		//  Area: Receptor

		MBPartner partner =  MBPartner.get(getCtx(), this.getC_BPartner_ID());
		MBPartnerLocation partnerLocation = new MBPartnerLocation(getCtx(), this.getC_BPartner_Location_ID(), get_TrxName());

		// OpenUp Ltda - #5627 - Raul Capecce - Si es una factura (o NC o ND) es obligatorio que tenga RUT
		/*
		if (
				cfeType.equals(CfeType.eFactura)
						|| cfeType.equals(CfeType.eFactura_NC)
						|| cfeType.equals(CfeType.eFactura_ND)
						|| cfeType.equals(CfeType.eFactura_VxCA)
						|| cfeType.equals(CfeType.eFactura_NC_VxCA)
						|| cfeType.equals(CfeType.eFactura_ND_VxCA)
				) {
			if (
					(partner.getDUNS() == null || partner.getDUNS().equalsIgnoreCase(""))
							|| (!partner.getDocumentType().equalsIgnoreCase(MBPartner.DOCUMENTTYPE_RUT))
					) {
				throw new AdempiereException(CFEMessages.RECEPTOR_FACTNORUT);
			}
		}
		*/
		// FIN - OpenUp Ltda - #5627

		int tipoDocRecep = 2;
		String docRecep = partner.getTaxID();

		/*
		if (partner.getDUNS() != null) {
			if (partner.getDocumentType().equalsIgnoreCase(MBPartner.DOCUMENTTYPE_RUT)) {
				tipoDocRecep = 2;
			} else {
				tipoDocRecep = 4;
			}
			docRecep = partner.getDUNS();
		} else if (partner.get_Value("cedula") != null) {
			tipoDocRecep = 3;
			docRecep = partner.get_Value("cedula").toString();
		} else {
			// OpenUp Ltda - #5834 - Raul Capecce - 21/04/2016
			// Si no se encuentra documento en el receptor, se retorna una excepci�n
			// tipoDocRecep = 4;
			// docRecep = CFEMessages.RECEPTOR_NODOC.replace("{{documentNo}}", mInvoice.getDocumentNo());
			throw new AdempiereException(CFEMessages.RECEPTOR_NODOC.replace("{{documentNo}}", mInvoice.getDocumentNo()));
			// OpenUp Ltda - #5834 - FIN
		}
		*/
		/* 60  */ receptorTck.setTipoDocRecep(tipoDocRecep);
		/* 60  */ receptorFact.setTipoDocRecep(tipoDocRecep);

		/*
		MCountry mCountry = null;
		try {
			mCountry = MCountry.get(ctx, Integer.valueOf(partnerLocation.get_Value("C_Country_ID").toString()));
		} catch (Exception e) {
			throw new AdempiereException(CFEMessages.RECEPTOR_61);
		}
		*/

		MCountry mCountry = null;
		mCountry = MCountry.get(getCtx(), 336);


		if (mCountry == null) throw new AdempiereException("CFEMessages.RECEPTOR_61");
		/* 61  */ receptorTck.setCodPaisRecep(mCountry.getCountryCode());
		/* 61  */ receptorFact.setCodPaisRecep(mCountry.getCountryCode());

		if (tipoDocRecep == 2 || tipoDocRecep == 3) {
			/* 62  */ receptorTck.setDocRecep(docRecep);
			/* 62  */ receptorFact.setDocRecep(docRecep);
		} else if (tipoDocRecep == 4 || tipoDocRecep == 5 || tipoDocRecep == 6) {
			/* 62.1*/ receptorTck.setDocRecepExt(docRecep);
			/* 62.1*/ receptorFact.setDocRecep(docRecep);
		}

		/* 63  */ receptorTck.setRznSocRecep(partner.getName2());
		/* 63  */ receptorFact.setRznSocRecep(partner.getName2());

		MLocation location = (MLocation) partnerLocation.getC_Location();
		String dirRecep="";
		String add1 = location.getAddress1();
		if (add1 != null) {
			if (add1.length() <= 70)
				dirRecep = add1;
			else
				dirRecep = add1.substring(0, 70);
		}
		/* 64  */ receptorTck.setDirRecep(dirRecep);
		/* 65  */ receptorTck.setCiudadRecep(location.getCity());
		/* 66  */ receptorTck.setDeptoRecep(location.getRegionName());
		/* 66.1*/ receptorTck.setPaisRecep("Uruguay");
		/* 64  */ receptorFact.setDirRecep(dirRecep);
		/* 65  */ receptorFact.setCiudadRecep(location.getCity());
		/* 66  */ receptorFact.setDeptoRecep(location.getRegionName());
		/* 66.1*/ receptorFact.setPaisRecep("Uruguay");

		try {
			///* 67  */ receptor.setCP(Integer.valueOf(partnerLocation.getUY_Localidades().getzipcode()));
		} catch (Exception ex) { }



		//  Area: Totales Encabezado

		MCurrency mCurrency = (MCurrency) this.getC_Currency();
		if (mCurrency.getISO_Code() == null) throw new AdempiereException("CFEMessages.TOTALES_110");
		try {
			/* 110 */ totales.setTpoMoneda(TipMonType.valueOf(mCurrency.getISO_Code()));
			if (mCurrency.getC_Currency_ID() != 142) {
				/* OpenUp Ltda - #5749 - Raul Capecce - Se toma la tasa de cambio de la factura, si no est� definida, se calcula para la fecha de la factura */
				/* OpenUp Ltda - #6138 - Raul Capecce - Se agrega preguntar por 0 y null */
				//if (mInvoice.getCurrencyRate() != null && !mInvoice.getCurrencyRate().equals(Env.ZERO)) {
					///* 111 */ totales.setTpoCambio(mInvoice.getCurrencyRate().setScale(3, BigDecimal.ROUND_HALF_UP));
//				} else {
					/* OpenUp Ltda - #5749 - Raul Capecce - Se calculaba al reves, ahora se calcula correctamente la tasa de conversion */
//					BigDecimal currRate = OpenUpUtils.getCurrencyRateForCur1Cur2(mInvoice.getDateInvoiced(), 142, mCurrency.getC_Currency_ID(), mInvoice.getAD_Client_ID(), mInvoice.getAD_Org_ID());
//					if (currRate.equals(Env.ZERO)) throw new AdempiereException(CFEMessages.TOTALES_111.replace("{{fecha}}", mInvoice.getDateInvoiced().toString()).replace("{{moneda}}", mCurrency.getISO_Code()));
					///* 111 */ totales.setTpoCambio(currRate.setScale(3, BigDecimal.ROUND_HALF_UP));
//				}
			}
		} catch (AdempiereException ex){
			throw ex;
		} catch (Exception ex){
			throw new AdempiereException("CFEMessages.TOTALES_110_2");
		}



		// Inicializando Variables - Se toma como referencia codigo establecido para Migrate
		/* 112 */ totales.setMntNoGrv(Env.ZERO);
		/* 113 - No establecido */ totales.setMntExpoyAsim(Env.ZERO);
		/* 114 - No establecido */ totales.setMntImpuestoPerc(Env.ZERO);
		/* 115 - No aparece en c�digo de Migrate */
		/* 116 */ totales.setMntNetoIvaTasaMin(Env.ZERO);
		/* 117 */ totales.setMntNetoIVATasaBasica(Env.ZERO);
		/* 118 */ totales.setMntNetoIVAOtra(Env.ZERO);

		/* 121 */ totales.setMntIVATasaMin(Env.ZERO);
		/* 122 */ totales.setMntIVATasaBasica(Env.ZERO);
		/* 123 */ totales.setMntIVAOtra(Env.ZERO);
		/* DocV16 - 129 */ totales.setMontoNF(Env.ZERO);

		/* OpenUp Ltda. - #7550 - Raul Capecce - 18/10/2016
		 * Se cargan los impuestos de las lineas que tienen tasa 0
		 * y despues se cargan los impuestos de la tabla C_InvoiceTax
		 * (en esta tabla solo estan los impuestos que tienen tasa mayor a 0)
		 *  */

		// Carga del campo C 124 segun Indicadores de Facturacion
		this.montoC124 = Env.ZERO;

		// Recorro las lineas para obtener los indicadores de Facturaci�n y tomar solamente los que tienen tasa 0
		MInvoiceLine[] invoiceLines = this.getLines(true);
		for (int i = 0; i < invoiceLines.length; i++){

			MInvoiceLine mInvL = invoiceLines[i];
			MTax tax = MTax.get(getCtx(), mInvL.getC_Tax_ID());

			if(tax == null) throw new AdempiereException("CFE Error: Area Totales Encabezado - Impuesto para linea no establecido");
			if(tax.getTaxIndicator() == null || tax.getTaxIndicator().equalsIgnoreCase("")) throw new AdempiereException("CFE Error: Area Totales Encabezado - Porcentaje de impuesto para la linea no establecido");
			BigDecimal taxIndicator = tax.getRate();


			/* Si tiene dos lineas, tomo el monto a manejar a partir del monto en territorio nacional
			 * y sumo el monto en territorio internacional en el tag Exportacion y Asimilados (ExpoyAsim).
			 * En caso de que se a una linea, tomo el monto del campo lineTotalAmt */
			BigDecimal netoMontoLinea = (BigDecimal) mInvL.get_Value("AmtSubtotal");
			BigDecimal ivaMontoLinea = mInvL.getTaxAmt();

			addTaxToTag(totales, tax, netoMontoLinea, ivaMontoLinea);

		}

		/* 119 */ totales.setIVATasaMin(new BigDecimal(10).setScale(3));
		/* 120 */ totales.setIVATasaBasica(new BigDecimal(22).setScale(3));

		/* 124 */ totales.setMntTotal(montoC124); // Total Monto Total


		// Validacion de Total a Pagar
//		BigDecimal validacionPagar = validacionPagar(totales);
//		if(mInvoice.getGrandTotal().compareTo(validacionPagar) == 0){
			/* 130 */ totales.setMntPagar(this.getGrandTotal());// Monto total a pagar
//		} else {
//			throw new AdempiereException("error en validar Monto total a pagar");
//		}

		// C126 - Validar que no exeda la cantidad de lineas admitida por cada tipo de CFE
		// eTicket (solo y con NC y ND): <= 700
		// Otros CFE: <= 200
		//if (cfeType == CfeType.eTicket || cfeType == CfeType.eTicket_NC || cfeType == CfeType.eTicket_ND
		//		|| cfeType == CfeType.eTicket_VxCA || cfeType == CfeType.eTicket_NC_VxCA || cfeType == CfeType.eTicket_ND_VxCA){
		//	if (invoiceLines.size() > 700) throw new AdempiereException(CFEMessages.TOTALES_126);
		//} else {
//			if (invoiceLines.size() > 200) throw new AdempiereException(CFEMessages.TOTALES_126_2);
		//}

		totales.setCantLinDet(invoiceLines.length);

	}

	private XMLGregorianCalendar Timestamp_to_XmlGregorianCalendar_OnlyDate(Timestamp timestamp, boolean withTime) {
		try {
			GregorianCalendar cal = (GregorianCalendar) GregorianCalendar.getInstance();
			cal.setTime(timestamp);
			XMLGregorianCalendar xgcal;
			if (!withTime){
				xgcal = DatatypeFactory.newInstance().newXMLGregorianCalendarDate(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH)+1, cal.get(Calendar.DAY_OF_MONTH), DatatypeConstants.FIELD_UNDEFINED);
			} else {
				xgcal = DatatypeFactory.newInstance().newXMLGregorianCalendarDate(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH)+1, cal.get(Calendar.DAY_OF_MONTH), DatatypeConstants.FIELD_UNDEFINED );
				xgcal.setHour(cal.get(Calendar.HOUR_OF_DAY));
				xgcal.setMinute(cal.get(Calendar.MINUTE));
				xgcal.setSecond(cal.get(Calendar.SECOND));
				xgcal.setMillisecond(cal.get(Calendar.MILLISECOND));
				xgcal.setTimezone(-3*60); // GTM -3 en minutos

			}
			return xgcal;
		} catch (DatatypeConfigurationException e) {
			throw new AdempiereException(e);
		}
	}

	private void addTaxToTag(Totales totales, MTax tax, BigDecimal netoMontoLinea, BigDecimal ivaMontoLinea) {
		if (tax.getRate().compareTo(Env.ZERO) == 0){
			/* 112 */ totales.setMntNoGrv(totales.getMntNoGrv().add(netoMontoLinea));
			/* 124 */ montoC124 = montoC124.add(netoMontoLinea);
		}
		else if (tax.getRate().compareTo(new BigDecimal(10)) == 0){
			/* 116 */ totales.setMntNetoIvaTasaMin(totales.getMntNetoIvaTasaMin().add(netoMontoLinea));
			/* 121 */ totales.setMntIVATasaMin(totales.getMntIVATasaMin().add(ivaMontoLinea));
			/* 124 */ montoC124 = montoC124.add(netoMontoLinea).add(ivaMontoLinea);
		}
		else if (tax.getRate().compareTo(new BigDecimal(22)) == 0){
			/* 117 */ totales.setMntNetoIVATasaBasica(totales.getMntNetoIVATasaBasica().add(netoMontoLinea));
			/* 122 */ totales.setMntIVATasaBasica(totales.getMntIVATasaBasica().add(ivaMontoLinea));
			/* 124 */ montoC124 = montoC124.add(netoMontoLinea).add(ivaMontoLinea);
		}
		else {
			/* 118 */ totales.setMntNetoIVAOtra(totales.getMntNetoIVAOtra().add(netoMontoLinea));
			/* 123 */ totales.setMntIVAOtra(totales.getMntIVAOtra().add(ivaMontoLinea));
			/* 124 */ montoC124 = montoC124.add(netoMontoLinea).add(ivaMontoLinea);
		}
	}


	private void loadDetalleProductosOServicios_eTicket_eFactura(String cfeType, CFEDefType objCfe) {


		MInvoiceLine[] mInvoiceLines = this.getLines(true);
		List<ItemDetFact> lineas = null;


		boolean isTaxIncluded = false;
		MPriceList priceList = (MPriceList) this.getM_PriceList();
		if (priceList != null && priceList.get_ValueAsBoolean("isTaxIncluded")) {
			isTaxIncluded = true;
		}

		/*
		if (cfeType == CfeType.eTicket || cfeType == CfeType.eTicket_ND || cfeType == CfeType.eTicket_NC
				|| cfeType == CfeType.eTicket_VxCA || cfeType == CfeType.eTicket_ND_VxCA || cfeType == CfeType.eTicket_NC_VxCA){
			objCfe.getETck().setDetalle(new ETck.Detalle());
			lineas = objCfe.getETck().getDetalle().getItem();
		} else if (cfeType == CfeType.eFactura || cfeType == CfeType.eFactura_ND || cfeType == CfeType.eFactura_NC
				|| cfeType == CfeType.eFactura_VxCA || cfeType == CfeType.eFactura_ND_VxCA || cfeType == CfeType.eFactura_NC_VxCA){
			objCfe.getEFact().setDetalle(new EFact.Detalle());
			lineas = objCfe.getEFact().getDetalle().getItem();
		}
		*/

		objCfe.getEFact().setDetalle(new CFEDefType.EFact.Detalle());
		lineas = objCfe.getEFact().getDetalle().getItem();

		int position = 1;
		for (int i = 0; i < mInvoiceLines.length; i++) {

			MInvoiceLine mInvoiceLine = mInvoiceLines[i];

			BigDecimal amtMntLinea = null;

			ItemDetFact detalleItem = new ItemDetFact();
			lineas.add(detalleItem);

			/* 1  */
			detalleItem.setNroLinDet(position++);
			MProduct mProduct = new MProduct(getCtx(), mInvoiceLine.getM_Product_ID(), get_TrxName());
			ItemDetFact.CodItem codItem = null;

			// Codigo interno del Cliente
			if (mProduct.getValue() != null) {
				codItem = new ItemDetFact.CodItem();
				/* 2  */
				codItem.setTpoCod("INT1");
				/* 3  */
				codItem.setCod(mProduct.getValue());
				detalleItem.getCodItem().add(codItem);
			}

			// Codigo EAN
			if (mProduct.getUPC() != null) {
				codItem = new ItemDetFact.CodItem();
				/* 2  */
				codItem.setTpoCod("EAN");
				/* 3  */
				codItem.setCod(mProduct.getUPC());
				detalleItem.getCodItem().add(codItem);
			}

			MTax tax = new MTax(getCtx(), mInvoiceLine.getC_Tax_ID(), null);
			if (tax.getRate().compareTo(new BigDecimal(10)) == 0) {
				/* 4  */
				detalleItem.setIndFact(BigInteger.valueOf(2));
			} else if (tax.getRate().compareTo(new BigDecimal(22)) == 0) {
				/* 4  */
				detalleItem.setIndFact(BigInteger.valueOf(3));

			/* OpenUp Ltda. Raul Capecce - #5873
			 * Nicolas Taurisano y Rodrigo Barbe informan que el iva percibidocarni debe ir como exento*/
				//} else if(tax.getValue().equalsIgnoreCase("percibidocarni")) {
				//	/* 4  */ detalleItem.setIndFact(BigInteger.valueOf(4));
			} else if (tax.getRate().compareTo(Env.ZERO) == 0) {
				/* 4  */
				detalleItem.setIndFact(BigInteger.valueOf(1));
			/* FIN - #5873 */

			/* 6  */
				detalleItem.setIndAgenteResp(null);

			/* 7  */
				detalleItem.setNomItem(mInvoiceLine.getProduct().getName());
			/* 8  */
				detalleItem.setDscItem(mInvoiceLine.getProduct().getDescription());
			/* 9  */
				detalleItem.setCantidad(mInvoiceLine.getQtyInvoiced());
			/* 10 */
				detalleItem.setUniMed(mInvoiceLine.getProduct().getUOMSymbol());

			/* OpenUp Ltda. - Raul Capecce - #6638
			 * Se establece valor absoluto del precio unitario, en el �nico caso de redoneo negativo, se pasa como positivo con indicador de facturaci�n 7 (negativo) en vez de 6 (positivo) */
			/* OpenUp Ltda. - Raul Capecce - #7531
			 * Se cambia PriceActual a PriceEntered */
				BigDecimal precioUnitario = mInvoiceLine.getPriceEntered().setScale(6, RoundingMode.HALF_UP).abs();

			/*
			 * OpenUp Ltda. - Raul Capecce - #7610 - 25/10/2016
			 * Se toma precio unitario directamente (ya viene con impuestos si corresponde)
			 */
//				if (!isTaxIncluded) {
//					BigDecimal taxIndicator = BigDecimal.valueOf(Double.valueOf(tax.getTaxIndicator().replace("%", "")));
//					precioUnitario = precioUnitario.multiply(Env.ONEHUNDRED.add(taxIndicator)).divide(Env.ONEHUNDRED).setScale(6, RoundingMode.HALF_UP);
//				}
				// OpenUp Ltda. - #7610 - Fin

			/* 11 */
				detalleItem.setPrecioUnitario(precioUnitario);



			/* OpenUp Ltda. - Raul Capecce - #7550
			 * Seg�n Nicolas Lopez, no se est�n aplicando descuentos a nivel de la linea en ningun cliente
			 *  */
//				/* OpenUp Ltda. - Raul Capecce - #6638
//				 * Como el redondeo es el �nico que puede utilizar los indicadores de facturaci�n 6 y 7 (Producto o Servicio No Facturable)
//				 * Se entiende que no se tienen que aplicar ningun descuento */
//				if (detalleItem.getIndFact().intValue() != 6 && detalleItem.getIndFact().intValue() != 7) {
//
//					if(mInvoiceLine.getFlatDiscount() != null && mInvoiceLine.getFlatDiscount().compareTo(Env.ZERO) > 0){
//						BigDecimal descuento = mInvoiceLine.getPriceActual().subtract(mInvoiceLine.getPriceEntered());
//						/* 12 */ detalleItem.setDescuentoPct(mInvoiceLine.getFlatDiscount());
//						/* 13 */ detalleItem.setDescuentoMonto(descuento);
//					}
//				}



			/* 14 - Tipo de obligatoriedad de tabla 3 (tabla opcional) */
			/* 15 - Tipo de obligatoriedad de tabla 3 (tabla opcional) */


			/* TODO: NOTA IMPORTANTE: RECARGO POR LINEA - campos 16 y 17 se marcan en 0 teniendose en cuenta
			 * de que no se aplicaran recargos por linea.
			 * Tener en cuenta que para agregar impuestos a la linea, falta agregar
			 * la columna de impuesto a la C_InvoiceLine (o manejar un descuento negativo).
			 */
			/* 16 */
				detalleItem.setRecargoPct(Env.ZERO);
			/* 17 */
				detalleItem.setRecargoMnt(Env.ZERO);

			/* 18 - Tipo de obligatoriedad de tabla 3 (tabla opcional) */
			/* 19 - Tipo de obligatoriedad de tabla 3 (tabla opcional) */


			/* TODO: CAMPOS 20, 21, 22 y 23 se dejan sin cargar
			 * al igual que los campos 127 y 128 del encabezado
			 */



			/* OpenUp Ltda. - #7550 - Raul Capecce - 18/10/2016
			 * Se pasa deja de calcular el monto del campo 24 */
//				/* 24 - C24=(C9*C11)-C13+C17 */
//				BigDecimal montoTotalLinea = Env.ZERO;
//				montoTotalLinea = montoTotalLinea.add(mInvoiceLine.getQtyInvoiced());
//
//				/* OpenUp Ltda. - Raul Capecce - #6638
//				 * Como el redondeo es el �nico que puede utilizar los indicadores de facturaci�n 6 y 7 (Producto o Servicio No Facturable)
//				 * Seg�n Soporte InvoiCy si es negativo se pasa a positivo pero con el indicador 7 */
//				/* OpenUp Ltda. - Raul Capecce - #7531 */
//				BigDecimal priceEntered = mInvoiceLine.getPriceEntered();
//				priceEntered = priceEntered.abs();
//				montoTotalLinea = montoTotalLinea.multiply(priceEntered.setScale(2, RoundingMode.HALF_UP)).setScale(2, RoundingMode.HALF_UP);
//				/* FIN #6638 */
//
//				if(mInvoiceLine.getFlatDiscount() != null && mInvoiceLine.getFlatDiscount().compareTo(Env.ZERO) > 0){
//					montoTotalLinea = montoTotalLinea.subtract(mInvoiceLine.getPriceActual().subtract(mInvoiceLine.getPriceEntered())).setScale(2, RoundingMode.HALF_UP);
//				}
//				montoTotalLinea = montoTotalLinea.add(detalleItem.getRecargoMnt()); // Aun no se toma en cuenta, por lo tanto mas arriba se setea en 0
//
//				/* OpenUp Ltda. - #7132 - Ra�l Capecce - 23/09/2016
//				 * El campo priceActual ya cuenta con el impuesto incluido si la lista de precios lo indica, por lo tanto se comenta la secci�n de c�digo que agrega redundantemente el impuesto a la liena del xml
//				 * */
////				/* OpenUp Ltda - #7034 - Ra�l Capecce - 16/08/2016
////				 * Se Incluye el impuesto a nivel de la linea si se indica en la lista de precios que los precios tienen impuestos incluidos. */
////				MPriceList priceList = (MPriceList) mInvoice.getM_PriceList();
////				if (priceList != null && priceList.get_ValueAsBoolean("isTaxIncluded")) {
////					MTax taxLine = (MTax) mInvoiceLine.getC_Tax();
////					if (taxLine != null) {
////						BigDecimal taxPorcentaje = BigDecimal.valueOf(Double.valueOf(tax.getTaxIndicator().replace("%", "")));
////						montoTotalLinea = montoTotalLinea.add(montoTotalLinea.multiply(taxPorcentaje).divide(Env.ONEHUNDRED).setScale(2, RoundingMode.HALF_UP)).setScale(2, RoundingMode.HALF_UP);
////					}
////				}
////				/* FIN #7034*/

			/*
			 * OpenUp Ltda. - #7610 - Raul Capecce - 25/10/2016
			 * Con impuestos incluidos tomo el total de linea, en caso contrario tomo el subtotal
			 */
			/*
			 * OpenUp Ltda. - #8560 - Raul Capecce - 01/02/2017
			 * Tomo el valor absoluto del total o subtotal debido a que siempre el precio unitario es positivo,
			 * el signo lo indica el indicador de facturacion (C24=(C9*C11)-C13+C17)
			 */
				if (isTaxIncluded) {
				/* 24 */
					detalleItem.setMontoItem(mInvoiceLine.getLineTotalAmt().abs());
				} else {
				/* 24 */
					detalleItem.setMontoItem(((BigDecimal) mInvoiceLine.get_Value("AmtSubtotal")).abs());
				}
			/* OpenUp Ltda. - #8560 - Fin */
			/* OpenUp Ltda. - #7610 - Fin */


			}


		}

	}

	private void loadCAE(CFEEmpresasType objECfe) {
		CFEDefType objCfe = objECfe.getCFE();

		CAEDataType caeDataType = new CAEDataType();
		objCfe.getEFact().setCAEData(caeDataType);

		caeDataType.setCAEID(new BigDecimal(90160029355.0).toBigInteger());
		caeDataType.setDNro(new BigDecimal(2000001).toBigInteger());
		caeDataType.setHNro(new BigDecimal(2100000).toBigInteger());
		caeDataType.setFecVenc(Timestamp_to_XmlGregorianCalendar_OnlyDate(Timestamp.valueOf("2018-03-03 00:00:00"), false));//mDgiCae.getfechaVencimiento() Emi

	}



	private void SendCfe(CFEDefType cfeDefType) {

		try {

			CFEEmpresasType cfeEmpresasType = new CFEEmpresasType();
			cfeEmpresasType.setCFE(cfeDefType);

			File file = File.createTempFile("SistecoXMLCFE", ".xml");
			file.deleteOnExit();
			JAXBContext jaxbContext = JAXBContext.newInstance(CFEEmpresasType.class);
			Marshaller jaxbMarshaller = jaxbContext.createMarshaller();


			jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

			jaxbMarshaller.marshal(cfeEmpresasType, file);

			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);

			String linea;
			String xml = "";
			while((linea=br.readLine())!=null) {
				xml += linea + "\n";
			}


//			// Quito namespaces
			xml = xml
//					//.replaceAll("xmlns:ns2=\"[a-zA-Z1-90:/.#]*\"", "")
//					//.replaceAll("xmlns:ns3=\"[a-zA-Z1-90:/.#]*\"", "")
//					//.replace("<CFE_Adenda  >", "<CFE_Adenda>")
//					//.replace("standalone=\"yes\"", "")
					.replace("<CFE xmlns:ns0=\"http://cfe.dgi.gub.uy\" version=\"1.0\">", "<ns0:CFE version=\"1.0\">")
					.replace("</CFE>","</ns0:CFE>")
					.replace("<CFE_Adenda ", "<ns0:CFE_Adenda xmlns:ns0=\"http://cfe.dgi.gub.uy\"")
					.replace("</CFE_Adenda>", "</ns0:CFE_Adenda>")
					.replace("xmlns:ns0=\"http://cfe.dgi.gub.uy\"xmlns:ns2=\"http://www.w3.org/2000/09/xmldsig#\"", "xmlns:ns0=\"http://cfe.dgi.gub.uy\" xmlns:ns2=\"http://www.w3.org/2000/09/xmldsig#\"");

			// Guardo XML sin los namespace
			PrintWriter pw = new PrintWriter(file);
			pw.println(xml);
			pw.close();


			Service service = new Service();
			Call call = (Call) service.createCall();
			// Establecemos la dirección en la que está activado el WebService
			call.setTargetEndpointAddress(new java.net.URL("http://10.0.0.130/ws_efactura/ws_efactura.php"));

			//call.setOperationName(new QName("efac", "http://www.objetos.com.uy/efactura/"));
			// Establecemos el nombre del método a invocar
			call.setOperationName(new QName("http://www.objetos.com.uy/efactura/", "recepcionDocumento"));
			call.setSOAPActionURI("http://www.objetos.com.uy/efactura/recepcionDocumento");

			// Establecemos los parámetros que necesita el método
			// Observe que se deben especidicar correctamente tanto el nómbre como el tipo de datos. Esta información se puede obtener viendo el WSDL del servicio Web
			call.addParameter(new QName("entrada"), XMLType.XSD_STRING, ParameterMode.IN);

			// Especificamos el tipo de datos que devuelve el método.
			call.setReturnType(XMLType.XSD_STRING);

			// Invocamos el método
			String result = (String) call.invoke("http://www.objetos.com.uy/efactura/", "recepcionDocumento", new Object[] { "<![CDATA[" + xml + "]]>" });

			// Quitamos el CDATA, solo al comienzo y al final si estan en el string
			result = result.replaceAll("^<!\\[CDATA\\[", "").replaceAll("]]>$", "");


			// Guardo la respuesta de Sisteco
			File response = File.createTempFile("SistecoXMLCFEResponse", ".xml");
			response.deleteOnExit();
			FileWriter fichero = new FileWriter(response);
			PrintWriter pwResponse = new PrintWriter(fichero);
			pwResponse.print(result);
			pwResponse.close();

			SistecoResponseDTO cfeDtoSisteco = SistecoConvertResponse.getObjSistecoResponseDTO(result);

			// Si la respuesta contiene errores, lanzo una excepci�n
			if (cfeDtoSisteco.getStatus() != 0) {
				throw new AdempiereException("CFEMessages.CFE_ERROR_PROVEEDOR : " + cfeDtoSisteco.getDescripcion());
			}


			/*
			MCFEDataEnvelope mCfeDataEnvelope = new MCFEDataEnvelope(getCtx(), 0, get_TrxName());
			mCfeDataEnvelope.setProviderAgent(MCFEDataEnvelope.PROVIDERAGENT_Sisteco);
			mCfeDataEnvelope.saveEx();

			PO docPo = (PO) cfeDto;
			MCFEDocCFE docCfe = new MCFEDocCFE(getCtx(), 0, get_TrxName());
			docCfe.setAD_Table_ID(docPo.get_Table_ID());
			docCfe.setRecord_ID(docPo.get_ID());
			docCfe.setUY_CFE_DataEnvelope_ID(mCfeDataEnvelope.get_ID());
			try {
				docCfe.setC_DocType_ID(BigDecimal.valueOf(docPo.get_ValueAsInt("C_DocTypeTarget_ID")));
				docCfe.setDocumentNo(docPo.get_ValueAsString("documentNo"));
			} catch (Exception e2) {}
			docCfe.saveEx();


			MCFESistecoSRspCFE sistecoCfeResp = new MCFESistecoSRspCFE(getCtx(), 0, get_TrxName());
			sistecoCfeResp.setCFEStatus(String.valueOf(cfeDtoSisteco.getStatus()));
			sistecoCfeResp.setCFEDescripcion(cfeDtoSisteco.getDescripcion());
			if (sistecoCfeResp.getCFEStatus().equalsIgnoreCase("0")) {
				sistecoCfeResp.setCFETipo(BigDecimal.valueOf(cfeDtoSisteco.getTipoCFE()));
				sistecoCfeResp.setCFESerie(cfeDtoSisteco.getSerie());
				sistecoCfeResp.setCFEMro(cfeDtoSisteco.getMro());
				//sistecoCfeResp.setCFETmstFirma(cfeDtoSisteco.getTmstFirma());
				sistecoCfeResp.setCFEDigestValue(cfeDtoSisteco.getDigestValue());
				sistecoCfeResp.setCFEResolucion(String.valueOf(cfeDtoSisteco.getResolucion()));
				sistecoCfeResp.setCFEAnioResolucion(BigDecimal.valueOf(cfeDtoSisteco.getAnioResolucion()));
				sistecoCfeResp.setCFEUrlDocumentoDGI(cfeDtoSisteco.getUrlDocumentoDGI());
				sistecoCfeResp.setCFECAEID(cfeDtoSisteco.getCaeId());
				sistecoCfeResp.setCFEDNro(cfeDtoSisteco.getdNro());
				sistecoCfeResp.setCFEHNro(cfeDtoSisteco.gethNro());
				//sistecoCfeResp.setCFEFecVenc(cfeDtoSisteco.getFecVenc());
			}
			sistecoCfeResp.setUY_CFE_DocCFE_ID(docCfe.get_ID());
			sistecoCfeResp.saveEx();
			*/

			MZCFERespuestaProvider cfeRespuesta = new MZCFERespuestaProvider(getCtx(), 0, get_TrxName());
			cfeRespuesta.setAD_Table_ID(I_C_Invoice.Table_ID);
			cfeRespuesta.setRecord_ID(this.get_ID());
			cfeRespuesta.setC_DocType_ID(this.getC_DocType_ID());
			cfeRespuesta.setDocumentNoRef(this.getDocumentNo());
			cfeRespuesta.setCFE_Status(String.valueOf(cfeDtoSisteco.getStatus()));
			cfeRespuesta.setCFE_Descripcion(cfeDtoSisteco.getDescripcion());
			if (cfeRespuesta.getCFE_Status().equalsIgnoreCase("0")){
				cfeRespuesta.setCFE_Tipo(BigDecimal.valueOf(cfeDtoSisteco.getTipoCFE()));
				cfeRespuesta.setCFE_Serie(cfeDtoSisteco.getSerie());
				cfeRespuesta.setCFE_Numero(cfeDtoSisteco.getMro());
				cfeRespuesta.setCFE_DigitoVerificador(cfeDtoSisteco.getDigestValue());
				cfeRespuesta.setCFE_Resolucion(String.valueOf(cfeDtoSisteco.getResolucion()));
				cfeRespuesta.setCFE_AnioResolucion(cfeDtoSisteco.getAnioResolucion());
				cfeRespuesta.setCFE_URL_DGI(cfeDtoSisteco.getUrlDocumentoDGI());
				cfeRespuesta.setCFE_CAE_ID(cfeDtoSisteco.getCaeId());
				cfeRespuesta.setCFE_NroInicial_CAE(cfeDtoSisteco.getdNro());
				cfeRespuesta.setCFE_NroFinal_CAE(cfeDtoSisteco.gethNro());
			}
			cfeRespuesta.saveEx();

		} catch (Exception e) {
			throw new AdempiereException(e);
		}

	}


}	//	MInvoice
