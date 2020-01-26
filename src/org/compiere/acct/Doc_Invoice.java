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
package org.compiere.acct;

import org.adempiere.exceptions.AdempiereException;
import org.compiere.Adempiere;
import org.compiere.model.*;
import org.compiere.util.DB;
import org.compiere.util.Env;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.logging.Level;

/**
 *  Post Invoice Documents.
 *  <pre>
 *  Table:              C_Invoice (318)
 *  Document Types:     ARI, ARC, ARF, API, APC
 *  </pre>
 *  @author Jorg Janke
 *  @author Armen Rizal, Goodwill Consulting
 *  	<li>BF: 2797257	Landed Cost Detail is not using allocation qty
 *  
 *  @version  $Id: Doc_Invoice.java,v 1.2 2006/07/30 00:53:33 jjanke Exp $
 */
public class Doc_Invoice extends Doc
{
	// Xpande. Gabriel Vila. 28/11/2018. Issue #3000
	// Flag para determinar si se hace el asiento normal según ADempiere o si por el contrario se hace el asiento según lineas manuales
	// de cuentas contables indicadas por el usuario en este documento.
	// Esto surge por asiento de carnes en retail, el cual es muy complejo y cambiante para hacerlo de manera automática.

	private boolean doAsientoManual = false;

	// Fin Xpande.




	/**
	 *  Constructor
	 * 	@param ass accounting schemata
	 * 	@param rs record
	 * 	@param trxName trx
	 */
	public Doc_Invoice(MAcctSchema[] ass, ResultSet rs, String trxName)
	{
		super (ass, MInvoice.class, rs, null, trxName);
	}	//	Doc_Invoice

	/** Contained Optional Tax Lines    */
	private DocTax[]        m_taxes = null;
	/** Currency Precision				*/
	private int				m_precision = -1;
	/** All lines are Service			*/
	private boolean			m_allLinesService = true;
	/** All lines are product item		*/
	private boolean			m_allLinesItem = true;

	/**
	 *  Load Specific Document Details
	 *  @return error message or null
	 */
	protected String loadDocumentDetails ()
	{
		MInvoice invoice = (MInvoice)getPO();
		setDateDoc(invoice.getDateInvoiced());
		setIsTaxIncluded(invoice.isTaxIncluded());
		setC_BPartner_ID(invoice.getC_BPartner_ID());

		// Xpande. Gabriel Vila. 28/11/2018. Issue #3000
		// Seteo flag de hacer o no asiento manual indicado por el usuario para este documento
		if (invoice.get_ValueAsBoolean("AsientoManualInvoice")){
			this.doAsientoManual = true;
		}
		// Fin Xpande

		//	Amounts
		setAmount(Doc.AMTTYPE_Gross, invoice.getGrandTotal());
		setAmount(Doc.AMTTYPE_Net, invoice.getTotalLines());
		setAmount(Doc.AMTTYPE_Charge, invoice.getChargeAmt());
				
		//	Contained Objects
		m_taxes = loadTaxes();
		p_lines = loadLines(invoice);

		log.fine("Lines=" + p_lines.length + ", Taxes=" + m_taxes.length);
		return null;
	}   //  loadDocumentDetails


	/***
	 * Crea lineas de asiento, según lineas de asiento manual ingresadas por el usuario en este documento.
	 * Xpande. Created by Gabriel Vila on 11/28/18.
	 * @param as
	 */
	private void createFactsAsientoManual (Fact fact, MAcctSchema as) {

		String sql = "";
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try{
			MInvoice invoice = (MInvoice)getPO();

			sql = " select * from z_invoiceastomanual where c_invoice_id =" + invoice.get_ID();

			pstmt = DB.prepareStatement(sql, this.getTrxName());
			rs = pstmt.executeQuery();

			while(rs.next()){
				MAccount account = new MAccount(getCtx(), rs.getInt("c_validcombination_id"), this.getTrxName());
				FactLine fl1 = fact.createLine (null, account, invoice.getC_Currency_ID(), rs.getBigDecimal("AmtSourceDr"), rs.getBigDecimal("AmtSourceCr"));
				if (fl1 != null){

					// Me aseguro organización correcta
					fl1.setAD_Org_ID(invoice.getAD_Org_ID());

					// Si en la linea del asiento manual tengo un socio de negocio distinto al socio de negocio del comprobante
					int cBPartnerIDAux = rs.getInt("c_bpartner_id");
					if (cBPartnerIDAux > 0){
						if (cBPartnerIDAux != invoice.getC_BPartner_ID()){
							// Guardo este socio de negocio en la linea del asiento contable
							fl1.setC_BPartner_ID(cBPartnerIDAux);
						}
					}
				}
			}
		}
		catch (Exception e){
			throw new AdempiereException(e);
		}
		finally {
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}

	}

	/**
	 *	Load Invoice Taxes
	 *  @return DocTax Array
	 */
	private DocTax[] loadTaxes()
	{
		ArrayList<DocTax> list = new ArrayList<DocTax>();
		String sql = "SELECT it.C_Tax_ID, t.Name, t.Rate, it.TaxBaseAmt, it.TaxAmt, t.IsSalesTax "
			+ "FROM C_Tax t, C_InvoiceTax it "
			+ "WHERE t.C_Tax_ID=it.C_Tax_ID AND it.C_Invoice_ID=?";
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try
		{
			pstmt = DB.prepareStatement(sql, getTrxName());
			pstmt.setInt(1, get_ID());
			rs = pstmt.executeQuery();
			//
			while (rs.next())
			{
				int C_Tax_ID = rs.getInt(1);
				String name = rs.getString(2);
				BigDecimal rate = rs.getBigDecimal(3);
				BigDecimal taxBaseAmt = rs.getBigDecimal(4);
				BigDecimal amount = rs.getBigDecimal(5);
				boolean salesTax = "Y".equals(rs.getString(6));
				//
				DocTax taxLine = new DocTax(C_Tax_ID, name, rate, 
					taxBaseAmt, amount, salesTax);
				log.fine(taxLine.toString());
				list.add(taxLine);
			}
		}
		catch (SQLException e)
		{
			log.log(Level.SEVERE, sql, e);
			return null;
		}
		finally {
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}

		//	Return Array
		DocTax[] tl = new DocTax[list.size()];
		list.toArray(tl);
		return tl;
	}	//	loadTaxes

	/**
	 *	Load Invoice Line
	 *	@param invoice invoice
	 *  @return DocLine Array
	 */
	private DocLine[] loadLines (MInvoice invoice)
	{
		ArrayList<DocLine> list = new ArrayList<DocLine>();
		//
		MInvoiceLine[] lines = invoice.getLines(false);
		for (int i = 0; i < lines.length; i++)
		{
			MInvoiceLine line = lines[i];
			if (line.isDescription())
				continue;
			DocLine docLine = new DocLine(line, this);
			//	Qty
			BigDecimal Qty = line.getQtyInvoiced();
			boolean cm = getDocumentType().equals(DOCTYPE_ARCredit) 
				|| getDocumentType().equals(DOCTYPE_APCredit);
			docLine.setQty(cm ? Qty.negate() : Qty, invoice.isSOTrx());
			//
			BigDecimal LineNetAmt = line.getLineNetAmt();
			BigDecimal PriceList = line.getPriceList();
			int C_Tax_ID = docLine.getC_Tax_ID();
			//	Correct included Tax
			if (isTaxIncluded() && C_Tax_ID != 0)
			{
				MTax tax = MTax.get(getCtx(), C_Tax_ID);
				if (!tax.isZeroTax())
				{
					BigDecimal LineNetAmtTax = tax.calculateTax(LineNetAmt, true, getStdPrecision());
					//BigDecimal LineNetAmtTax = tax.calculateTax(LineNetAmt, true, 4);
					log.fine("LineNetAmt=" + LineNetAmt + " - Tax=" + LineNetAmtTax);
					LineNetAmt = LineNetAmt.subtract(LineNetAmtTax);
					for (int t = 0; t < m_taxes.length; t++)
					{
						if (m_taxes[t].getC_Tax_ID() == C_Tax_ID)
						{
							m_taxes[t].addIncludedTax(LineNetAmtTax);
							break;
						}
					}
					BigDecimal PriceListTax = tax.calculateTax(PriceList, true, getStdPrecision());
					//BigDecimal PriceListTax = tax.calculateTax(PriceList, true, 4);
					PriceList = PriceList.subtract(PriceListTax);
				}
			}	//	correct included Tax
			
			docLine.setAmount (LineNetAmt, PriceList, Qty);	//	qty for discount calc
			if (docLine.isItem())
				m_allLinesService = false;
			else
				m_allLinesItem = false;
			//
			log.fine(docLine.toString());
			list.add(docLine);
		}
		
		//	Convert to Array
		DocLine[] dls = new DocLine[list.size()];
		list.toArray(dls);

		//	Included Tax - make sure that no difference
		if (isTaxIncluded())
		{
			for (int i = 0; i < m_taxes.length; i++)
			{
				if (m_taxes[i].isIncludedTaxDifference())
				{
					BigDecimal diff = m_taxes[i].getIncludedTaxDifference(); 
					for (int j = 0; j < dls.length; j++)
					{
						if (dls[j].getC_Tax_ID() == m_taxes[i].getC_Tax_ID())
						{
							dls[j].setLineNetAmtDifference(diff);
							break;
						}
					}	//	for all lines
				}	//	tax difference
			}	//	for all taxes
		}	//	Included Tax difference
		
		//	Return Array
		return dls;
	}	//	loadLines

	/**
	 * 	Get Currency Precision
	 *	@return precision
	 */
	private int getStdPrecision()
	{
		if (m_precision == -1)
			m_precision = MCurrency.getStdPrecision(getCtx(), getC_Currency_ID());
		return m_precision;
	}	//	getPrecision

	
	/**************************************************************************
	 *  Get Source Currency Balance - subtracts line and tax amounts from total - no rounding
	 *  @return positive amount, if total invoice is bigger than lines
	 */
	public BigDecimal getBalance()
	{

		BigDecimal retValue = Env.ZERO;
		StringBuffer sb = new StringBuffer (" [");

		//  Total
		retValue = retValue.add(getAmount(Doc.AMTTYPE_Gross));
		sb.append(getAmount(Doc.AMTTYPE_Gross));
		//  - Header Charge
		retValue = retValue.subtract(getAmount(Doc.AMTTYPE_Charge));
		sb.append("-").append(getAmount(Doc.AMTTYPE_Charge));

		// Xpande. Gabriel Vila. 28/11/2018.
		// Si tengo flag de generar asiento manual segun lineas de asiento ingresadas por el usuario.
		// Agrego el IF y le meto dentro el codigo original de ADempiere
		if (!this.doAsientoManual){
			//  - Tax
			for (int i = 0; i < m_taxes.length; i++)
			{
				retValue = retValue.subtract(m_taxes[i].getAmount());
				sb.append("-").append(m_taxes[i].getAmount());
			}
			//  - Lines
			for (int i = 0; i < p_lines.length; i++)
			{
				retValue = retValue.subtract(p_lines[i].getAmtSource());
				sb.append("-").append(p_lines[i].getAmtSource());
			}
		}
		else{
			// Considero lineas de asiento manual para el balanceo
			BigDecimal balanceo = this.getBalanceAsientoManual();
			if (balanceo.compareTo(Env.ZERO) > 0){
				retValue = retValue.subtract(balanceo);
			}
			else{
				retValue = retValue.add(balanceo);
			}
		}
		//retValue = Env.ZERO;
		// Fin Xpande.

		sb.append("]");
		//
		log.fine(toString() + " Balance=" + retValue + sb.toString());
		return retValue;
	}   //  getBalance

	/**
	 *  Create Facts (the accounting logic) for
	 *  ARI, ARC, ARF, API, APC.
	 *  <pre>
	 *  ARI, ARF
	 *      Receivables     DR
	 *      Charge                  CR
	 *      TaxDue                  CR
	 *      Revenue                 CR
	 *
	 *  ARC
	 *      Receivables             CR
	 *      Charge          DR
	 *      TaxDue          DR
	 *      Revenue         RR
	 *
	 *  API
	 *      Payables                CR
	 *      Charge          DR
	 *      TaxCredit       DR
	 *      Expense         DR
	 *
	 *  APC
	 *      Payables        DR
	 *      Charge                  CR
	 *      TaxCredit               CR
	 *      Expense                 CR
	 *  </pre>
	 *  @param as accounting schema
	 *  @return Fact
	 */
	public ArrayList<Fact> createFacts (MAcctSchema as)
	{
		//
		ArrayList<Fact> facts = new ArrayList<Fact>();
		//  create Fact Header
		Fact fact = new Fact(this, as, Fact.POST_Actual);

		//  Cash based accounting
		if (!as.isAccrual())
			return facts;

		//  ** ARI, ARF
		if (getDocumentType().equals(DOCTYPE_ARInvoice) 
			|| getDocumentType().equals(DOCTYPE_ARProForma))
		{

			BigDecimal grossAmt = getAmount(Doc.AMTTYPE_Gross);
			BigDecimal serviceAmt = Env.ZERO;

			// Xpande. Gabriel Vila. 04/04/2019.
			// Defino flag para contabilizar normalmente o no el credito.
			boolean contabilizaCR = true;

			/*
			// Si aplica facturación entre locales, no hago la contabilización normal para Credito.
			if (this.contabilizaVtaEntreLocales(fact, grossAmt, true)){
				contabilizaCR = false;
			}
			// Fin Xpande.
			 */

			// Xpande. Gabriel Vila. 04/04/2019.
			// Si contabilizo el crédito normalmente
			if (contabilizaCR){
				// Fin Xpande.

				//  Header Charge           CR
				BigDecimal amt = getAmount(Doc.AMTTYPE_Charge);
				if (amt != null && amt.signum() != 0)
					fact.createLine(null, getAccount(Doc.ACCTTYPE_Charge, as),
							getC_Currency_ID(), null, amt);
				//  TaxDue                  CR
				for (int i = 0; i < m_taxes.length; i++)
				{
					amt = m_taxes[i].getAmount();
					if (amt != null)
					{
						FactLine tl = fact.createLine(null, m_taxes[i].getAccount(DocTax.ACCTTYPE_TaxDue, as),
								getC_Currency_ID(), null, amt);
						if (tl != null){
							tl.setC_Tax_ID(m_taxes[i].getC_Tax_ID());
						}
						// Xpande. Gabriel Vila. 03/10/2019.
						// Mejoro mensajes de error cuando no se encuentra la cuenta parametrizada
						else {
							MAccount accountTax = m_taxes[i].getAccount(m_taxes[i].getAPTaxType(), as);
							if ((accountTax == null) || (accountTax.get_ID() <= 0)) {
								p_Error = "No se obtuvo cuenta contable de venta para impuesto : " + m_taxes[i].getName();
								log.log(Level.SEVERE, p_Error);
								fact = null;
								facts.add(fact);
								return facts;
							}
						}
						// Fin Xpande
					}
				}
				//  Revenue                 CR
				for (int i = 0; i < p_lines.length; i++)
				{
					amt = p_lines[i].getAmtSource();
					BigDecimal dAmt = null;
					if (as.isTradeDiscountPosted())
					{
						BigDecimal discount = p_lines[i].getDiscount();
						if (discount != null && discount.signum() != 0)
						{
							amt = amt.add(discount);
							dAmt = discount;
							fact.createLine (p_lines[i],
									p_lines[i].getAccount(ProductCost.ACCTTYPE_P_TDiscountGrant, as),
									getC_Currency_ID(), dAmt, null);
						}
					}
					fact.createLine (p_lines[i],
							p_lines[i].getAccount(ProductCost.ACCTTYPE_P_Revenue, as),
							getC_Currency_ID(), null, amt);
					if (!p_lines[i].isItem())
					{
						grossAmt = grossAmt.subtract(amt);
						serviceAmt = serviceAmt.add(amt);
					}
				}

			}

			//  Set Locations
			FactLine[] fLines = fact.getLines();
			for (int i = 0; i < fLines.length; i++)
			{
				if (fLines[i] != null)
				{
					fLines[i].setLocationFromOrg(fLines[i].getAD_Org_ID(), true);      //  from Loc
					fLines[i].setLocationFromBPartner(getC_BPartner_Location_ID(), false);  //  to Loc
				}
			}
			
			//  Receivables     DR
			int receivables_ID = getValidCombination_ID(Doc.ACCTTYPE_C_Receivable, as);
			int receivablesServices_ID = getValidCombination_ID (Doc.ACCTTYPE_C_Receivable_Services, as);
			if (m_allLinesItem || !as.isPostServices() 
				|| receivables_ID == receivablesServices_ID)
			{
				grossAmt = getAmount(Doc.AMTTYPE_Gross);
				serviceAmt = Env.ZERO;
			}
			else if (m_allLinesService)
			{
				serviceAmt = getAmount(Doc.AMTTYPE_Gross);
				grossAmt = Env.ZERO;
			}
			if (grossAmt.signum() != 0)
				fact.createLine(null, MAccount.get(getCtx(), receivables_ID),
					getC_Currency_ID(), grossAmt, null);
			if (serviceAmt.signum() != 0)
				fact.createLine(null, MAccount.get(getCtx(), receivablesServices_ID),
					getC_Currency_ID(), serviceAmt, null);
		}
		//  ARC
		else if (getDocumentType().equals(DOCTYPE_ARCredit))
		{
			BigDecimal grossAmt = getAmount(Doc.AMTTYPE_Gross);
			BigDecimal serviceAmt = Env.ZERO;

			// Xpande. Gabriel Vila. 04/04/2019.
			// Defino flag para contabilizar normalmente o no el debito.
			boolean contabilizaDR = true;

			/*
			// Si aplica facturación entre locales, no hago la contabilización normal para Credito.
			if (this.contabilizaVtaEntreLocales(fact, grossAmt, false)){
				contabilizaDR = false;
			}
			// Fin Xpande.
			 */

			// Xpande. Gabriel Vila. 04/04/2019.
			// Si contabilizo el crédito normalmente
			if (contabilizaDR){
				// Fin Xpande

				//  Header Charge   DR
				BigDecimal amt = getAmount(Doc.AMTTYPE_Charge);
				if (amt != null && amt.signum() != 0)
					fact.createLine(null, getAccount(Doc.ACCTTYPE_Charge, as),
							getC_Currency_ID(), amt, null);
				//  TaxDue          DR
				for (int i = 0; i < m_taxes.length; i++)
				{
					amt = m_taxes[i].getAmount();
					if (amt != null)
					{
						FactLine tl = fact.createLine(null, m_taxes[i].getAccount(DocTax.ACCTTYPE_TaxDue, as),
								getC_Currency_ID(), amt, null);
						if (tl != null)
							tl.setC_Tax_ID(m_taxes[i].getC_Tax_ID());
					}
				}
				//  Revenue         CR
				for (int i = 0; i < p_lines.length; i++)
				{
					amt = p_lines[i].getAmtSource();
					BigDecimal dAmt = null;
					if (as.isTradeDiscountPosted())
					{
						BigDecimal discount = p_lines[i].getDiscount();
						if (discount != null && discount.signum() != 0)
						{
							amt = amt.add(discount);
							dAmt = discount;
							fact.createLine (p_lines[i],
									p_lines[i].getAccount (ProductCost.ACCTTYPE_P_TDiscountGrant, as),
									getC_Currency_ID(), null, dAmt);
						}
					}
					fact.createLine (p_lines[i],
							p_lines[i].getAccount (ProductCost.ACCTTYPE_P_Revenue, as),
							getC_Currency_ID(), amt, null);
					if (!p_lines[i].isItem())
					{
						grossAmt = grossAmt.subtract(amt);
						serviceAmt = serviceAmt.add(amt);
					}
				}
			}

			//  Set Locations
			FactLine[] fLines = fact.getLines();
			for (int i = 0; i < fLines.length; i++)
			{
				if (fLines[i] != null)
				{
					fLines[i].setLocationFromOrg(fLines[i].getAD_Org_ID(), true);      //  from Loc
					fLines[i].setLocationFromBPartner(getC_BPartner_Location_ID(), false);  //  to Loc
				}
			}
			//  Receivables             CR
			int receivables_ID = getValidCombination_ID (Doc.ACCTTYPE_C_Receivable, as);
			int receivablesServices_ID = getValidCombination_ID (Doc.ACCTTYPE_C_Receivable_Services, as);
			if (m_allLinesItem || !as.isPostServices() 
				|| receivables_ID == receivablesServices_ID)
			{
				grossAmt = getAmount(Doc.AMTTYPE_Gross);
				serviceAmt = Env.ZERO;
			}
			else if (m_allLinesService)
			{
				serviceAmt = getAmount(Doc.AMTTYPE_Gross);
				grossAmt = Env.ZERO;
			}
			if (grossAmt.signum() != 0)
				fact.createLine(null, MAccount.get(getCtx(), receivables_ID),
					getC_Currency_ID(), null, grossAmt);
			if (serviceAmt.signum() != 0)
				fact.createLine(null, MAccount.get(getCtx(), receivablesServices_ID),
					getC_Currency_ID(), null, serviceAmt);
		}
		
		//  ** API
		else if (getDocumentType().equals(DOCTYPE_APInvoice))
		{
			BigDecimal grossAmt = getAmount(Doc.AMTTYPE_Gross);
			BigDecimal serviceAmt = Env.ZERO;

			//  Charge          DR
			fact.createLine(null, getAccount(Doc.ACCTTYPE_Charge, as),
				getC_Currency_ID(), getAmount(Doc.AMTTYPE_Charge), null);

			// Xpande. Gabriel Vila. 28/11/2018.
			// Si tengo flag de generar asiento manual segun lineas de asiento ingresadas por el usuario.
			// Agrego el IF y le meto dentro el codigo original de ADempiere
			if (!this.doAsientoManual){

				//  TaxCredit       DR
				for (int i = 0; i < m_taxes.length; i++)
				{
					// Xpande. Gabriel Vila. 21/03/2019.
					// Mejoro mensajes de error cuando no se encuentra la cuenta parametrizada
					MAccount accountTax = m_taxes[i].getAccount(m_taxes[i].getAPTaxType(), as);
					if ((accountTax == null) || (accountTax.get_ID() <= 0)){
						p_Error = "No se obtuvo cuenta contable de compra para impuesto : " + m_taxes[i].getName();
						log.log(Level.SEVERE, p_Error);
						fact = null;
						facts.add(fact);
						return facts;
					}
					// Fin Xpande

					FactLine tl;
					if (m_taxes[i].getRate().signum() >= 0)
						tl = fact.createLine(null, m_taxes[i].getAccount(m_taxes[i].getAPTaxType(), as), getC_Currency_ID(), m_taxes[i].getAmount(), null);
					else
						tl = fact.createLine(null, m_taxes[i].getAccount(m_taxes[i].getAPTaxType(), as), getC_Currency_ID(),  null , m_taxes[i].getAmount().negate());

					if (tl != null)
						tl.setC_Tax_ID(m_taxes[i].getC_Tax_ID());
				}
				//  Expense         DR
				for (int i = 0; i < p_lines.length; i++)
				{
					DocLine line = p_lines[i];
					boolean landedCost = landedCost(as, fact, line, true);
					if (landedCost && as.isExplicitCostAdjustment())
					{
						fact.createLine (line, line.getAccount(ProductCost.ACCTTYPE_P_Expense, as),
								getC_Currency_ID(), line.getAmtSource(), null);
						//
						FactLine fl = fact.createLine (line, line.getAccount(ProductCost.ACCTTYPE_P_Expense, as),
								getC_Currency_ID(), null, line.getAmtSource());
						String desc = line.getDescription();
						if (desc == null)
							desc = "100%";
						else
							desc += " 100%";
						fl.setDescription(desc);
					}
					if (!landedCost)
					{
						MAccount expense = line.getAccount(ProductCost.ACCTTYPE_P_Expense, as);
						if (line.isItem())
							expense = line.getAccount (ProductCost.ACCTTYPE_P_InventoryClearing, as);

						// Xpande. Gabriel Vila. 21/03/2019.
						// Mejoro mensajes de error cuando no se encuentra la cuenta parametrizada
						if ((expense == null) || (expense.get_ID() <= 0)){
							String msgCta = "P_Expense_Acct";
							String msgProd = "";
							if (line.isItem()){
								msgCta = "P_InventoryClearing_Acct";
							}
							if (line.getM_Product_ID() > 0){
								MProduct product = new MProduct(getCtx(), line.getM_Product_ID(), null);
								msgProd = "Producto : " + product.getValue() + " - " + product.getName();
							}
							else if(line.getC_Charge_ID() > 0){
								MCharge charge = new MCharge(getCtx(), line.getC_Charge_ID(), null);
								msgProd = "Cargo : " + charge.getName();
							}
							p_Error = "No se obtuvo cuenta contable de compra (" + msgCta + "). " + msgProd;
							log.log(Level.SEVERE, p_Error);
							fact = null;
							facts.add(fact);
							return facts;
						}
						// Fin Xpande

						BigDecimal amt = line.getAmtSource();
						BigDecimal dAmt = null;
						if (as.isTradeDiscountPosted() && !line.isItem())
						{
							BigDecimal discount = line.getDiscount();
							if (discount != null && discount.signum() != 0)
							{
								amt = amt.add(discount);
								dAmt = discount;
								MAccount tradeDiscountReceived = line.getAccount(ProductCost.ACCTTYPE_P_TDiscountRec, as);
								fact.createLine (line, tradeDiscountReceived,
										getC_Currency_ID(), null, dAmt);
							}
						}
						fact.createLine (line, expense,
								getC_Currency_ID(), amt, null);
						if (!line.isItem())
						{
							grossAmt = grossAmt.subtract(amt);
							serviceAmt = serviceAmt.add(amt);
						}
						//
					/*if (line.getM_Product_ID() != 0
						&& line.getProduct().isService())	//	otherwise Inv Matching
						MCostDetail.createInvoice(as, line.getAD_Org_ID(),
							line.getM_Product_ID(), line.getM_AttributeSetInstance_ID(),
							line.get_ID(), 0,		//	No Cost Element
							line.getAmtSource(), line.getQty(),
							line.getDescription(), getTrxName());*/
					}
				}
				//  Set Locations
				FactLine[] fLines = fact.getLines();
				for (int i = 0; i < fLines.length; i++)
				{
					if (fLines[i] != null)
					{
						fLines[i].setLocationFromBPartner(getC_BPartner_Location_ID(), true);  //  from Loc
						fLines[i].setLocationFromOrg(fLines[i].getAD_Org_ID(), false);    //  to Loc
					}
				}

			}
			else{
				// Hago el asiento manual para sustiuir impuestos y lineas del documento.
				this.createFactsAsientoManual(fact, as);
				this.setC_BPartner_ID(((MInvoice)getPO()).getC_BPartner_ID());
			}
			// Fin Xpande.

			//  Liability               CR
			int payables_ID = getValidCombination_ID (Doc.ACCTTYPE_V_Liability, as);
			int payablesServices_ID = getValidCombination_ID (Doc.ACCTTYPE_V_Liability_Services, as);
			if (m_allLinesItem || !as.isPostServices() 
				|| payables_ID == payablesServices_ID)
			{
				grossAmt = getAmount(Doc.AMTTYPE_Gross);
				serviceAmt = Env.ZERO;
			}
			else if (m_allLinesService)
			{
				serviceAmt = getAmount(Doc.AMTTYPE_Gross);
				grossAmt = Env.ZERO;
			}
			if (grossAmt.signum() != 0){

				// Xpande. Gabriel Vila. 21/03/2019.
				// Mejoro mensajes de error cuando no se encuentra la cuenta parametrizada
				MAccount acctBP = MAccount.get(getCtx(), payables_ID);
				if ((acctBP == null) || (acctBP.get_ID() <= 0)){
					p_Error = "No se obtuvo cuenta contable de compra (V_Liability_Acct) para el Socio de Negocio.";
					log.log(Level.SEVERE, p_Error);
					fact = null;
					facts.add(fact);
					return facts;

				}
				// Fin Xpande

				fact.createLine(null, MAccount.get(getCtx(), payables_ID),
						getC_Currency_ID(), null, grossAmt);
			}
			if (serviceAmt.signum() != 0){

				// Xpande. Gabriel Vila. 21/03/2019.
				// Mejoro mensajes de error cuando no se encuentra la cuenta parametrizada
				MAccount acctBP = MAccount.get(getCtx(), payables_ID);
				if ((acctBP == null) || (acctBP.get_ID() <= 0)){
					p_Error = "No se obtuvo cuenta contable de compra (V_Liability_Services_Acct) para el Socio de Negocio.";
					log.log(Level.SEVERE, p_Error);
					fact = null;
					facts.add(fact);
					return facts;

				}
				// Fin Xpande

				fact.createLine(null, MAccount.get(getCtx(), payablesServices_ID),
						getC_Currency_ID(), null, serviceAmt);
			}
			//
			updateProductPO(as);	//	Only API
			updateProductInfo (as.getC_AcctSchema_ID());    //  only API
		}
		//  APC
		else if (getDocumentType().equals(DOCTYPE_APCredit))
		{
			BigDecimal grossAmt = getAmount(Doc.AMTTYPE_Gross);
			BigDecimal serviceAmt = Env.ZERO;
			//  Charge                  CR
			fact.createLine (null, getAccount(Doc.ACCTTYPE_Charge, as),
				getC_Currency_ID(), null, getAmount(Doc.AMTTYPE_Charge));

			// Xpande. Gabriel Vila. 28/11/2018.
			// Si tengo flag de generar asiento manual segun lineas de asiento ingresadas por el usuario.
			// Agrego el IF y le meto dentro el codigo original de ADempiere
			if (!this.doAsientoManual){
				//  TaxCredit               CR
				for (int i = 0; i < m_taxes.length; i++)
				{
					FactLine tl;
					if (m_taxes[i].getRate().signum() >= 0)
						tl = fact.createLine (null, m_taxes[i].getAccount(m_taxes[i].getAPTaxType(), as), getC_Currency_ID(), null, m_taxes[i].getAmount());
					else
						tl = fact.createLine (null, m_taxes[i].getAccount(m_taxes[i].getAPTaxType(), as), getC_Currency_ID(), m_taxes[i].getAmount().negate(),null);

					if (tl != null)
						tl.setC_Tax_ID(m_taxes[i].getC_Tax_ID());
				}
				//  Expense                 CR
				for (int i = 0; i < p_lines.length; i++)
				{
					DocLine line = p_lines[i];
					boolean landedCost = landedCost(as, fact, line, false);
					if (landedCost && as.isExplicitCostAdjustment())
					{
						fact.createLine (line, line.getAccount(ProductCost.ACCTTYPE_P_Expense, as),
								getC_Currency_ID(), null, line.getAmtSource());
						//
						FactLine fl = fact.createLine (line, line.getAccount(ProductCost.ACCTTYPE_P_Expense, as),
								getC_Currency_ID(), line.getAmtSource(), null);
						String desc = line.getDescription();
						if (desc == null)
							desc = "100%";
						else
							desc += " 100%";
						fl.setDescription(desc);
					}
					if (!landedCost)
					{
						MAccount expense = line.getAccount(ProductCost.ACCTTYPE_P_Expense, as);
						if (line.isItem())
							expense = line.getAccount (ProductCost.ACCTTYPE_P_InventoryClearing, as);
						BigDecimal amt = line.getAmtSource();
						BigDecimal dAmt = null;
						if (as.isTradeDiscountPosted() && !line.isItem())
						{
							BigDecimal discount = line.getDiscount();
							if (discount != null && discount.signum() != 0)
							{
								amt = amt.add(discount);
								dAmt = discount;
								MAccount tradeDiscountReceived = line.getAccount(ProductCost.ACCTTYPE_P_TDiscountRec, as);
								fact.createLine (line, tradeDiscountReceived,
										getC_Currency_ID(), dAmt, null);
							}
						}
						fact.createLine (line, expense,
								getC_Currency_ID(), null, amt);
						if (!line.isItem())
						{
							grossAmt = grossAmt.subtract(amt);
							serviceAmt = serviceAmt.add(amt);
						}
						//
					/*if (line.getM_Product_ID() != 0
						&& line.getProduct().isService())	//	otherwise Inv Matching
						MCostDetail.createInvoice(as, line.getAD_Org_ID(),
							line.getM_Product_ID(), line.getM_AttributeSetInstance_ID(),
							line.get_ID(), 0,		//	No Cost Element
							line.getAmtSource().negate(), line.getQty(),
							line.getDescription(), getTrxName());*/
					}
				}
				//  Set Locations
				FactLine[] fLines = fact.getLines();
				for (int i = 0; i < fLines.length; i++)
				{
					if (fLines[i] != null)
					{
						fLines[i].setLocationFromBPartner(getC_BPartner_Location_ID(), true);  //  from Loc
						fLines[i].setLocationFromOrg(fLines[i].getAD_Org_ID(), false);    //  to Loc
					}
				}
			}
			else{
				// Hago el asiento manual para sustiuir impuestos y lineas del documento.
				this.createFactsAsientoManual(fact, as);
				this.setC_BPartner_ID(((MInvoice)getPO()).getC_BPartner_ID());
			}
			// Fin Xpande.

			//  Liability       DR
			int payables_ID = getValidCombination_ID (Doc.ACCTTYPE_V_Liability, as);
			int payablesServices_ID = getValidCombination_ID (Doc.ACCTTYPE_V_Liability_Services, as);
			if (m_allLinesItem || !as.isPostServices() 
				|| payables_ID == payablesServices_ID)
			{
				grossAmt = getAmount(Doc.AMTTYPE_Gross);
				serviceAmt = Env.ZERO;
			}
			else if (m_allLinesService)
			{
				serviceAmt = getAmount(Doc.AMTTYPE_Gross);
				grossAmt = Env.ZERO;
			}
			if (grossAmt.signum() != 0)
				fact.createLine(null, MAccount.get(getCtx(), payables_ID),
					getC_Currency_ID(), grossAmt, null);
			if (serviceAmt.signum() != 0)
				fact.createLine(null, MAccount.get(getCtx(), payablesServices_ID),
					getC_Currency_ID(), serviceAmt, null);
		}
		else
		{
			p_Error = "DocumentType unknown: " + getDocumentType();
			log.log(Level.SEVERE, p_Error);
			fact = null;
		}
		//
		facts.add(fact);
		return facts;
	}   //  createFact


	/***
	 * Contabiliza venta entre locales si aplica.
	 * @param fact
	 * @param grossAmt
	 * @return
	 */
	private boolean contabilizaVtaEntreLocales(Fact fact, BigDecimal grossAmt, boolean isCR) {

		boolean result = false;

		try{

			MBPartner partner = new MBPartner(getCtx(), this.getC_BPartner_ID(), null);

			// Si el socio de negocio de este comprobante, es a su vez una organización de la misma empresa (venta entre locales)
			if (partner.getAD_OrgBP_ID_Int() > 0){

				// Si la organización asociada al socio de negocio esta parametrizada para utilizar cuenta transitoria en venta entre locales
				String sql = " select count(*) " +
						     " from z_comconfvtalocalorg  " +
							 " where ad_orgtrx_id =" + getAD_Org_ID() +
							 " and isactive='Y'";
				int cont = DB.getSQLValueEx(null, sql);
				if (cont > 0){

					// Obtengo cuenta transitoria
					sql = " select max(p_revenue_acct) as p_revenue_acct from z_comconfvtalocalacct where ad_client_id =" + this.getAD_Client_ID();
					int accountID = DB.getSQLValueEx(null, sql);
					if (accountID <= 0){
						throw new AdempiereException("Falta parametrizar cuenta transitoria para venta entre locales, en configuraciones comerciales.");
					}
					MAccount acct = MAccount.get(getCtx(), accountID);
					if (acct == null){
						throw new AdempiereException("No existe la cuenta transitoria para venta entre locales definida en configuraciones comerciales.");
					}

					// Registro para del asiento contables con cuenta contable transitoria y monto recibido
					if (isCR){
						fact.createLine (null, acct, getC_Currency_ID(), null, grossAmt);
					}
					else{
						fact.createLine (null, acct, getC_Currency_ID(), grossAmt, null);
					}

					result = true;
				}
			}

		}
		catch (Exception e){
		    throw new AdempiereException(e);
		}

		return result;
	}

	/**
	 * 	Create Fact Cash Based (i.e. only revenue/expense)
	 *	@param as accounting schema
	 *	@param fact fact to add lines to
	 *	@param multiplier source amount multiplier
	 *	@return accounted amount
	 */
	public BigDecimal createFactCash (MAcctSchema as, Fact fact, BigDecimal multiplier)
	{
		boolean creditMemo = getDocumentType().equals(DOCTYPE_ARCredit)
			|| getDocumentType().equals(DOCTYPE_APCredit);
		boolean payables = getDocumentType().equals(DOCTYPE_APInvoice)
			|| getDocumentType().equals(DOCTYPE_APCredit);
		BigDecimal acctAmt = Env.ZERO;
		FactLine fl = null;
		//	Revenue/Cost
		for (int i = 0; i < p_lines.length; i++)
		{
			DocLine line = p_lines[i];
			boolean landedCost = false;
			if  (payables)
				landedCost = landedCost(as, fact, line, false);
			if (landedCost && as.isExplicitCostAdjustment())
			{
				fact.createLine (line, line.getAccount(ProductCost.ACCTTYPE_P_Expense, as),
					getC_Currency_ID(), null, line.getAmtSource());
				//
				fl = fact.createLine (line, line.getAccount(ProductCost.ACCTTYPE_P_Expense, as),
					getC_Currency_ID(), line.getAmtSource(), null);
				String desc = line.getDescription();
				if (desc == null)
					desc = "100%";
				else
					desc += " 100%";
				fl.setDescription(desc);
			}
			if (!landedCost)
			{
				MAccount acct = line.getAccount(
					payables ? ProductCost.ACCTTYPE_P_Expense : ProductCost.ACCTTYPE_P_Revenue, as);
				if (payables)
				{
					//	if Fixed Asset
					if (line.isItem())
						acct = line.getAccount (ProductCost.ACCTTYPE_P_InventoryClearing, as);
				}
				BigDecimal amt = line.getAmtSource().multiply(multiplier);
				BigDecimal amt2 = null;
				if (creditMemo)
				{
					amt2 = amt;
					amt = null;
				}
				if (payables)	//	Vendor = DR
					fl = fact.createLine (line, acct,
						getC_Currency_ID(), amt, amt2);
				else			//	Customer = CR
					fl = fact.createLine (line, acct,
						getC_Currency_ID(), amt2, amt);
				if (fl != null)
					acctAmt = acctAmt.add(fl.getAcctBalance());
			}
		}
		//  Tax
		for (int i = 0; i < m_taxes.length; i++)
		{
			BigDecimal amt = m_taxes[i].getAmount();
			BigDecimal amt2 = null;
			if (creditMemo)
			{
				amt2 = amt;
				amt = null;
			}
			FactLine tl = null;
			if (payables)
				tl = fact.createLine (null, m_taxes[i].getAccount(m_taxes[i].getAPTaxType(), as),
					getC_Currency_ID(), amt, amt2);
			else
				tl = fact.createLine (null, m_taxes[i].getAccount(DocTax.ACCTTYPE_TaxDue, as),
					getC_Currency_ID(), amt2, amt);
			if (tl != null)
				tl.setC_Tax_ID(m_taxes[i].getC_Tax_ID());
		}
		//  Set Locations
		FactLine[] fLines = fact.getLines();
		for (int i = 0; i < fLines.length; i++)
		{
			if (fLines[i] != null)
			{
				if (payables)
				{
					fLines[i].setLocationFromBPartner(getC_BPartner_Location_ID(), true);  //  from Loc
					fLines[i].setLocationFromOrg(fLines[i].getAD_Org_ID(), false);    //  to Loc
				}
				else
				{
					fLines[i].setLocationFromOrg(fLines[i].getAD_Org_ID(), true);    //  from Loc
					fLines[i].setLocationFromBPartner(getC_BPartner_Location_ID(), false);  //  to Loc
				}
			}
		}
		return acctAmt;
	}	//	createFactCash
	
	
	/**
	 * 	Create Landed Cost accounting & Cost lines
	 *	@param as accounting schema
	 *	@param fact fact
	 *	@param line document line
	 *	@param isDebit DR entry (normal api)
	 *	@return true if landed costs were created
	 */
	private boolean landedCost (MAcctSchema as, Fact fact, DocLine line, boolean isDebit)
	{
		int invoiceLineId = line.get_ID();
		MLandedCostAllocation[] landedCostAllocations = MLandedCostAllocation.getOfInvoiceLine(
			getCtx(), invoiceLineId, getTrxName());
		if (landedCostAllocations.length == 0)
			return false;

		BigDecimal totalBase = Arrays.stream(landedCostAllocations)
				.map(MLandedCostAllocation::getBase)
				.reduce(BigDecimal.ZERO, BigDecimal::add);

		//	Create New
		MInvoiceLine invoiceLine = new MInvoiceLine (getCtx(), invoiceLineId, getTrxName());
		Arrays.stream(landedCostAllocations)
				.filter(landedCostAllocation -> landedCostAllocation.getBase().signum() != 0) // only cost allocation with base > 0
				.forEach(landedCostAllocation -> {
			BigDecimal percent = landedCostAllocation.getBase().divide(totalBase, BigDecimal.ROUND_HALF_UP);
			String desc = invoiceLine.getDescription();
			if (desc == null)
				desc = percent + "%";
			else
				desc += " - " + percent + "%";
			if (line.getDescription() != null)
				desc += " - " + line.getDescription();
			
			//	Accounting
			ProductCost productCost = new ProductCost (Env.getCtx(),
				landedCostAllocation.getM_Product_ID(), landedCostAllocation.getM_AttributeSetInstance_ID(), getTrxName());
			BigDecimal debitAmount = BigDecimal.ZERO;
			BigDecimal creditAmount = BigDecimal.ZERO;;
			FactLine factLine = null;
			MCostType costType = MCostType.get(as, landedCostAllocation.getM_Product_ID() , landedCostAllocation.getAD_Org_ID());
			if(MCostType.COSTINGMETHOD_AverageInvoice.equals(costType.getCostingMethod()))
			{
				//Cost to inventory asset
				BigDecimal assetAmount = Optional.ofNullable(MCostDetail.getByDocLineLandedCost(
						landedCostAllocation,
						as.getC_AcctSchema_ID(),
						costType.get_ID())).orElse(BigDecimal.ZERO);
				//cost to Cost Adjustment
				BigDecimal costAdjustment = landedCostAllocation.getAmt().subtract(assetAmount);
				if (assetAmount.signum() != 0)
				{
					if (isDebit)
						debitAmount = assetAmount;
					else
						creditAmount = assetAmount;

					factLine = fact.createLine(line, productCost.getAccount(ProductCost.ACCTTYPE_P_Asset, as),
							as.getC_Currency_ID(), debitAmount, creditAmount);
					factLine.setDescription(desc + " " + landedCostAllocation.getM_CostElement().getName());
					factLine.setM_Product_ID(landedCostAllocation.getM_Product_ID());
				}
				if (costAdjustment.signum() != 0) {
					if (isDebit)
						debitAmount = costAdjustment;
					else
						creditAmount = costAdjustment;

					factLine = fact.createLine(line, productCost.getAccount(ProductCost.ACCTTYPE_P_CostAdjustment,as),
							getC_Currency_ID(), debitAmount, creditAmount);
				}
			}	
			else
			{	
				factLine = fact.createLine (line, productCost.getAccount(ProductCost.ACCTTYPE_P_CostAdjustment, as),
						getC_Currency_ID(), debitAmount, creditAmount);
			}	
			
			factLine.setDescription(desc + " " + landedCostAllocation.getM_CostElement().getName());
			factLine.setM_Product_ID(landedCostAllocation.getM_Product_ID());
		});
		log.config("Created #" + landedCostAllocations.length);
		return true;
	}	//	landedCosts

	/**
	 * 	Update ProductPO PriceLastInv
	 *	@param as accounting schema
	 */
	private void updateProductPO (MAcctSchema as)
	{
		MClientInfo ci = MClientInfo.get(getCtx(), as.getAD_Client_ID());
		if (ci.getC_AcctSchema1_ID() != as.getC_AcctSchema_ID())
			return;
		
		StringBuffer sql = new StringBuffer (
			"UPDATE M_Product_PO po "
			+ "SET PriceLastInv = "
			//	select
			+ "(SELECT currencyConvert(il.PriceActual,i.C_Currency_ID,po.C_Currency_ID,i.DateInvoiced,i.C_ConversionType_ID,i.AD_Client_ID,i.AD_Org_ID) "
			+ "FROM C_Invoice i, C_InvoiceLine il "
			+ "WHERE i.C_Invoice_ID=il.C_Invoice_ID"
			+ " AND po.M_Product_ID=il.M_Product_ID AND po.C_BPartner_ID=i.C_BPartner_ID");
			//jz + " AND ROWNUM=1 AND i.C_Invoice_ID=").append(get_ID()).append(") ")
			if (DB.isOracle()) //jz
			{
				sql.append(" AND ROWNUM=1 ");
			}
			else 
			{
				sql.append(" AND il.C_InvoiceLine_ID = (SELECT MIN(il1.C_InvoiceLine_ID) "
						+ "FROM C_Invoice i1, C_InvoiceLine il1 "
						+ "WHERE i1.C_Invoice_ID=il1.C_Invoice_ID"
						+ " AND po.M_Product_ID=il1.M_Product_ID AND po.C_BPartner_ID=i1.C_BPartner_ID")
						.append("  AND i1.C_Invoice_ID=").append(get_ID()).append(") ");
			}
			sql.append("  AND i.C_Invoice_ID=").append(get_ID()).append(") ")
			//	update
			.append("WHERE EXISTS (SELECT * "
			+ "FROM C_Invoice i, C_InvoiceLine il "
			+ "WHERE i.C_Invoice_ID=il.C_Invoice_ID"
			+ " AND po.M_Product_ID=il.M_Product_ID AND po.C_BPartner_ID=i.C_BPartner_ID"
			+ " AND i.C_Invoice_ID=").append(get_ID()).append(")");
		int no = DB.executeUpdate(sql.toString(), getTrxName());
		log.fine("Updated=" + no);
	}	//	updateProductPO
	
	/**
	 *  Update Product Info (old).
	 *  - Costing (PriceLastInv)
	 *  - PO (PriceLastInv)
	 *  @param C_AcctSchema_ID accounting schema
	 *  @deprecated old costing
	 */
	private void updateProductInfo (int C_AcctSchema_ID)
	{
		log.fine("C_Invoice_ID=" + get_ID());

		/** @todo Last.. would need to compare document/last updated date
		 *  would need to maintain LastPriceUpdateDate on _PO and _Costing */

		//  update Product Costing
		//  requires existence of currency conversion !!
		//  if there are multiple lines of the same product last price uses first
		//	-> TotalInvAmt is sometimes NULL !! -> error
		// begin globalqss 2005-10-19
		// postgresql doesn't support LIMIT on UPDATE or DELETE statements
		/*
		StringBuffer sql = new StringBuffer (
			"UPDATE M_Product_Costing pc "
			+ "SET (PriceLastInv, TotalInvAmt,TotalInvQty) = "
			//	select
			+ "(SELECT currencyConvert(il.PriceActual,i.C_Currency_ID,a.C_Currency_ID,i.DateInvoiced,i.C_ConversionType_ID,i.AD_Client_ID,i.AD_Org_ID),"
			+ " currencyConvert(il.LineNetAmt,i.C_Currency_ID,a.C_Currency_ID,i.DateInvoiced,i.C_ConversionType_ID,i.AD_Client_ID,i.AD_Org_ID),il.QtyInvoiced "
			+ "FROM C_Invoice i, C_InvoiceLine il, C_AcctSchema a "
			+ "WHERE i.C_Invoice_ID=il.C_Invoice_ID"
			+ " AND pc.M_Product_ID=il.M_Product_ID AND pc.C_AcctSchema_ID=a.C_AcctSchema_ID"
			+ " AND ROWNUM=1"
			+ " AND pc.C_AcctSchema_ID=").append(C_AcctSchema_ID).append(" AND i.C_Invoice_ID=")
			.append(get_ID()).append(") ")
			//	update
			.append("WHERE EXISTS (SELECT * "
			+ "FROM C_Invoice i, C_InvoiceLine il, C_AcctSchema a "
			+ "WHERE i.C_Invoice_ID=il.C_Invoice_ID"
			+ " AND pc.M_Product_ID=il.M_Product_ID AND pc.C_AcctSchema_ID=a.C_AcctSchema_ID"
			+ " AND pc.C_AcctSchema_ID=").append(C_AcctSchema_ID).append(" AND i.C_Invoice_ID=")
				.append(get_ID()).append(")");
		*/
		// the next command is equivalent and works in postgresql and oracle
		StringBuffer sql = new StringBuffer (
				"UPDATE M_Product_Costing pc "
				+ "SET (PriceLastInv, TotalInvAmt,TotalInvQty) = "
				//	select
				+ "(SELECT currencyConvert(il.PriceActual,i.C_Currency_ID,a.C_Currency_ID,i.DateInvoiced,i.C_ConversionType_ID,i.AD_Client_ID,i.AD_Org_ID),"
				+ " currencyConvert(il.LineNetAmt,i.C_Currency_ID,a.C_Currency_ID,i.DateInvoiced,i.C_ConversionType_ID,i.AD_Client_ID,i.AD_Org_ID),il.QtyInvoiced "
				+ "FROM C_Invoice i, C_InvoiceLine il, C_AcctSchema a "
				+ "WHERE i.C_Invoice_ID=il.C_Invoice_ID"
				+ " AND il.c_invoiceline_id = (SELECT MIN(C_InvoiceLine_ID) FROM C_InvoiceLine il2" +
						" WHERE  il2.M_PRODUCT_ID=il.M_PRODUCT_ID AND C_Invoice_ID=")
				.append(get_ID()).append(")"
				+ " AND pc.M_Product_ID=il.M_Product_ID AND pc.C_AcctSchema_ID=a.C_AcctSchema_ID"
				+ " AND pc.C_AcctSchema_ID=").append(C_AcctSchema_ID).append(" AND i.C_Invoice_ID=")
				.append(get_ID()).append(") ")
				//	update
				.append("WHERE EXISTS (SELECT * "
				+ "FROM C_Invoice i, C_InvoiceLine il, C_AcctSchema a "
				+ "WHERE i.C_Invoice_ID=il.C_Invoice_ID"
				+ " AND pc.M_Product_ID=il.M_Product_ID AND pc.C_AcctSchema_ID=a.C_AcctSchema_ID"
				+ " AND pc.C_AcctSchema_ID=").append(C_AcctSchema_ID).append(" AND i.C_Invoice_ID=")
					.append(get_ID()).append(")");
		// end globalqss 2005-10-19
		int no = DB.executeUpdate(sql.toString(), getTrxName());
		log.fine("M_Product_Costing - Updated=" + no);
	}   //  updateProductInfo


	/***
	 * Retorna balanceo para asiento manual ingresado por el usuario para este documento
	 * Xpande. Created by Gabriel Vila on 11/28/18.
	 * @return
	 */
	private BigDecimal getBalanceAsientoManual(){

		BigDecimal retValue = Env.ZERO;

		String sql = "";
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try{
			MInvoice invoice = (MInvoice)getPO();

			sql = " select coalesce(amtacctdr,0) as amtacctdr, coalesce(amtacctcr,0) as amtacctcr from z_invoiceastomanual where c_invoice_id =" + invoice.get_ID();

			pstmt = DB.prepareStatement(sql, this.getTrxName());
			rs = pstmt.executeQuery();

			while(rs.next()){
				retValue = retValue.add(rs.getBigDecimal("AmtAcctDr").subtract(rs.getBigDecimal("AmtAcctCr")));
			}
		}
		catch (Exception e){
		    throw new AdempiereException(e);
		}
		finally {
		    DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}

		return retValue;
	}

}   //  Doc_Invoice
