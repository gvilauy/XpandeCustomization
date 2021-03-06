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

import org.compiere.model.*;
import org.compiere.util.DB;
import org.compiere.util.Env;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.logging.Level;

/**
 *  Post GL Journal Documents.
 *  <pre>
 *  Table:              GL_Journal (224)
 *  Document Types:     GLJ
 *  </pre>
 *  @author Jorg Janke
 *  @version  $Id: Doc_GLJournal.java,v 1.3 2006/07/30 00:53:33 jjanke Exp $
 */
public class Doc_GLJournal extends Doc
{
	/**
	 *  Constructor
	 * 	@param ass accounting schemata
	 * 	@param rs record
	 * 	@param trxName trx
	 */
	public Doc_GLJournal(MAcctSchema[] ass, ResultSet rs, String trxName)
	{
		super(ass, MJournal.class, rs, null, trxName);
	}	//	Doc_GL_Journal

	/** Posting Type				*/
	private String			m_PostingType = null;
	private int				m_C_AcctSchema_ID = 0;
	
	/**
	 *  Load Specific Document Details
	 *  @return error message or null
	 */
	protected String loadDocumentDetails ()
	{
		MJournal journal = (MJournal)getPO();
		m_PostingType = journal.getPostingType();
		m_C_AcctSchema_ID = journal.getC_AcctSchema_ID();
			
		//	Contained Objects
		p_lines = loadLines(journal);
		log.fine("Lines=" + p_lines.length);
		return null;
	}   //  loadDocumentDetails


	/**
	 *	Load Invoice Line
	 *	@param journal journal
	 *  @return DocLine Array
	 */
	private DocLine[] loadLines(MJournal journal)
	{
		ArrayList<DocLine> list = new ArrayList<DocLine>();
		MJournalLine[] lines = journal.getLines(false);

		// Xpande. Gabriel Vila. 17/11/2018.
		// Seteo multimoneda cuando tengo monedas distintas en las lineas
		int cCurrencyID = 0;
		// Fin Xpande

		for (int i = 0; i < lines.length; i++)
		{
			MJournalLine line = lines[i];
			DocLine docLine = new DocLine (line, this); 
			//  --  Source Amounts
			docLine.setAmount (line.getAmtSourceDr(), line.getAmtSourceCr());
			//  --  Converted Amounts
			docLine.setConvertedAmt (m_C_AcctSchema_ID, line.getAmtAcctDr(), line.getAmtAcctCr());
			// -- qty
			docLine.setQty(line.getQty(), false);
			//  --  Account
			MAccount account = line.getAccount_Combi();
			docLine.setAccount (account);
			//	--	Organization of Line was set to Org of Account
			list.add(docLine);

			// Xpande. Gabriel Vila. 17/11/2018.
			// Seteo multimoneda cuando tengo monedas distintas en las lineas
			if (cCurrencyID != 0){
				if (line.getC_Currency_ID() != cCurrencyID){
					this.setIsMultiCurrency(true);
				}
			}
			else{
				cCurrencyID = line.getC_Currency_ID();
			}
			// Fin Xpande

		}
		//	Return Array
		int size = list.size();
		DocLine[] dls = new DocLine[size];
		list.toArray(dls);
		return dls;
	}	//	loadLines

	
	/**************************************************************************
	 *  Get Source Currency Balance - subtracts line and tax amounts from total - no rounding
	 *  @return positive amount, if total invoice is bigger than lines
	 */
	public BigDecimal getBalance()
	{
		BigDecimal retValue = Env.ZERO;
		StringBuffer sb = new StringBuffer (" [");
		//  Lines
		for (int i = 0; i < p_lines.length; i++)
		{
			// Xpande. Gabriel Vila. 16/11/2018.
			// Controlo balance utilizando los campos AmtAcct y no los AmtSource, ya que si hay multimoneda no me sirve.
			// Comento y sustituyo.

			// retValue = retValue.add(p_lines[i].getAmtSource());
			retValue = retValue.add(p_lines[i].getAmtAcctDr().subtract(p_lines[i].getAmtAcctCr()));

			// Fin Xpande.

			sb.append("+").append(p_lines[i].getAmtSource());
		}
		sb.append("]");
		//
		log.fine(toString() + " Balance=" + retValue + sb.toString());
		return retValue;
	}   //  getBalance

	/**
	 *  Create Facts (the accounting logic) for
	 *  GLJ.
	 *  (only for the accounting scheme, it was created)
	 *  <pre>
	 *      account     DR          CR
	 *  </pre>
	 *  @param as acct schema
	 *  @return Fact
	 */
	public ArrayList<Fact> createFacts (MAcctSchema as)
	{
		ArrayList<Fact> facts = new ArrayList<Fact>();
		//	Other Acct Schema
		if (as.getC_AcctSchema_ID() != m_C_AcctSchema_ID)
			return facts;
		
		//  create Fact Header
		Fact fact = new Fact (this, as, m_PostingType);

		MJournal journal = (MJournal)getPO();

		//  GLJ
		if (getDocumentType().equals(DOCTYPE_GLJournal))
		{
			//  account     DR      CR
			for (int i = 0; i < p_lines.length; i++)
			{
				if (p_lines[i].getC_AcctSchema_ID () == as.getC_AcctSchema_ID ())
				{
					// Xpande. Gabriel Vila. 16/11/2018.
					// Comento contabilización original de ADempiere donde se utiliza la moneda del cabezal,
					// y la sustituyo por la moneda de cada linea.
					// Comento y sustituyo.

					/*
					FactLine line = fact.createLine (p_lines[i],
									p_lines[i].getAccount (),
									getC_Currency_ID(),
									p_lines[i].getAmtSourceDr (),
									p_lines[i].getAmtSourceCr ());
					*/

					FactLine line = fact.createLine (p_lines[i],
							p_lines[i].getAccount (),
							p_lines[i].getC_Currency_ID(),
							p_lines[i].getAmtSourceDr (),
							p_lines[i].getAmtSourceCr ());

					if (line != null){

						line.setAD_Org_ID(journal.getAD_Org_ID());

						// Instancio modelo de linea de asiento manual
						MJournalLine journalLine = new MJournalLine(getCtx(), p_lines[i].get_ID(), this.getTrxName());
						if ((journalLine != null) && (journalLine.get_ID() > 0)){

							// Si tengo fecha de vencimiento, la seteo en la linea de asiento
							Timestamp dueDate = (Timestamp) journalLine.get_Value("DueDate");
							if (dueDate != null){
								line.set_ValueOfColumn("DueDate", dueDate);
							}

							// Si tengo tasa de cambio, la seteo en la linea del asiento
							BigDecimal currencyRate = journalLine.getCurrencyRate();
							if ((currencyRate != null) && (currencyRate.compareTo(Env.ZERO) > 0)){
								line.set_ValueOfColumn("CurrencyRate", currencyRate);
							}
						}

						line.saveEx();

						// Impacto detalle contable en caso de tener seteado impuesto o retencion.
						String strTaxID = "null", strRetencionID = "null";
						if (journalLine.get_ValueAsInt("C_Tax_ID") > 0){
							strTaxID = String.valueOf(journalLine.get_ValueAsInt("C_Tax_ID"));
						}
						if (journalLine.get_ValueAsInt("Z_RetencionSocio_ID") > 0){
							strRetencionID = String.valueOf(journalLine.get_ValueAsInt("Z_RetencionSocio_ID"));
						}

						if ((!strTaxID.equalsIgnoreCase("null")) || (!strRetencionID.equalsIgnoreCase("null"))){

							MSequence sequence = MSequence.get(getCtx(), "Z_AcctFactDet");

							String action = " insert into z_acctfactdet (z_acctfactdet_id, ad_client_id, ad_org_id, created, createdby, updated, updatedby, isactive, " +
									"fact_acct_id, c_tax_id, z_retencionsocio_id, gl_journal_id) ";
							String sql = " select nextid(" + sequence.get_ID() + ",'N'), ad_client_id, ad_org_id, created, createdby, updated, updatedby, isactive, " +
									line.get_ID() + ", " + strTaxID + ", " + strRetencionID + ", " + journal.get_ID() +
									" from fact_acct " +
									" where fact_acct_id =" + line.get_ID();
							DB.executeUpdateEx(action + sql, getTrxName());
						}
					}
					// Fin Xpande

				}
			}	//	for all lines
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

}   //  Doc_GLJournal
