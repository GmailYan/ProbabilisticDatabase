using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using ProbabilisticDatabase.Src.ControllerPackage;

namespace ProbabilisticDatabase
{
    public partial class Form1 : Form
    {
        private IAnalyticEngine DBengine = new AnalyticEngine();

        public Form1()
        {
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            UserControl1 a = new UserControl1();
            this.Controls.Add(a);
            this.button1.Text = "I had been clicked";
            errorProvider1.SetError(button1, "clicked error");
        }

        private void button2_Click(object sender, EventArgs e)
        {
            switch (submitSQL.Text)
            {
                case "New Query":
                    submitSQL.Text = "Submit";
                    textBox2.Enabled = true;
                    break;
                case "View Result":
                    tabControl1.SelectTab("tabPage3");
                    submitSQL.Text = "New Query";
                    break;
                case "Submit":
                    DataTable result;
                    textBox2.Text = DBengine.submitQuerySQL(textBox2.Text,out result);
                    setStatusLabel(textBox2.Text);
                    textBox2.Enabled = false;
                    submitSQL.Text = "View Result"; 
                    if (result!= null && result.Rows.Count >0)
                    {
                        setGridViewContent( result);
                    }
                    break;
                default:
                    textBox2.Text = "button is in wrong state,please contact admin";
                    break;
            }

        }

        private void button1_Click_1(object sender, EventArgs e)
        {
            string tableName = textBox1.Text;
            DataTable result = DBengine.viewTable(tableName);
            dataGridView1.DataSource = result;
            dataGridView1.Refresh();
        }

        private void setGridViewContent(DataTable content)
        {
            dataGridView1.DataSource = content;
        }

        private void setStatusLabel(string status)
        {
            toolStripLabel2.Text = status;
        }

    }
}
