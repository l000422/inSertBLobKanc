using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
using System.IO;

namespace inSertBLobKanc
{
    class Program
    {
        static void Main(string[] args)
        {

            using (OracleConnection ora = new OracleConnection(@"Data Source = 172.23.33.6:1521/orcl; Persist Security Info = True;User ID=l000422;Password=vfrfhtyrj1984;"))
            {
                ora.Open();
                OracleCommand cmd = new OracleCommand("select A.T_FILENAME, A.n_Selfs$,a.n_self$ from IRA.T#DOC#OFFICE O " +
                    "inner join IRA.T#DOC#OFFICE_APPENDIX A on (A.n_Owners$, A.n_Owner$) = ((O.N_SELFS$, O.n_Self$)) " +
                    "WHERE O.D_BEGIN$ >= trunc(sysdate - 1) " +
                    "and O.d_End$ is null and A.d_End$ is null and A.T_USER$ = 'A999999'", ora);
                OracleDataAdapter da = new OracleDataAdapter(cmd);
                DataTable dt = new DataTable();
                da.Fill(dt);
                
                for (var i = 0; i < dt.Rows.Count; i++) 
                {
                    FileStream fs = new FileStream(dt.Rows[i][0].ToString(), FileMode.Open, FileAccess.Read);
                    string nFile = Path.GetFileName(dt.Rows[i][0].ToString());
                    BinaryReader rd = new BinaryReader(fs);
                    int streamLength = (int)fs.Length;


                    OracleCommand commando = new OracleCommand("IRA.PK#DOC#FILE#INSERT_OFFICE.P#INSERT_BLOB", ora)
                    {
                        CommandType = CommandType.StoredProcedure
                    };               

                    commando.Parameters.Add("PN_SELFS", OracleDbType.Int32).Value = Convert.ToInt32(dt.Rows[i][1]); ;
                    commando.Parameters.Add("PN_SELF", OracleDbType.Int32).Value = Convert.ToInt32(dt.Rows[i][2]); ;
                    commando.Parameters.Add("PBLOB_IN", OracleDbType.Blob).Value = rd.ReadBytes(streamLength);
                    commando.Parameters.Add("PFILENAME", OracleDbType.Varchar2).Value = nFile;
                    commando.ExecuteNonQuery();

                }
            }
            
        }

    } 

        
    
}
