
package exa17oraclemongo;

import java.text.NumberFormat;

 public class Pedido17
{
	private String codcli;
        private String codpro;

	private Double cantidade;
	private String data;

	public Pedido17()
	{
		this("", "",0.0, "");
	}

	public Pedido17(String codcli, String codpro, Double cantidade, String data)
	{
		this.codcli = codcli;
                this.codpro = codpro;
                this.cantidade = cantidade;
		this.data = data;
	}

	public void setCodcli(String codcli)
	{
		this.codcli = codcli;
	}

	public String getCodcli(){
		return codcli;
	}
        public void setCodpro(String codpro)
	{
		this.codpro = codpro;
	}

	public String getCodpro(){
		return codpro;
	}
               
	public void setCantidade(double cantidade)
	{
		this.cantidade = cantidade;
	}

	public Double getCantidade()
	{
		return cantidade;
	}

	public void setData(String data)
	{
		this.data = data;
	}

	public String getData()
	{
		return data;
	}

	public String getFormattedCantidade()
	{
		NumberFormat currency = NumberFormat.getCurrencyInstance();
		return currency.format(cantidade);
	}

    
	public String toString()
	{
		return "Codcli:        " + codcli + "\n" +
                        "Codpro:        " + codpro + "\n" +
			   "Cantidade: " +this.getFormattedCantidade() + "\n" +
			   "Data:       " + data + "\n";
	}
}