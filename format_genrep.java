import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;


public class format_genrep extends EvalFunc<String> {
	@Override
    public String exec(Tuple input) throws IOException {
		// TODO Auto-generated method stub
		if(input == null)
		{
			return null;
		}
		try{
			String  str = (String) input.get(0);//get input and transfer to string
			String out = "";
			if(str.contains("|"))
			{//check whether it is a complex attribute
				String [] s = str.split("\\|");//"|" is a special symbol in regex
				
				for(int i =0; i<s.length;i++)
				{
					if(i==(s.length-1))
					{
						out+=("&"+(i+1)+")"+s[i]+" yxc135530");
					}
					else
					{
						out+=((i+1)+")"+s[i]+",");
					}
				}
				return out;
			}
			out = "1)"+str+" yxc135530";
			return out;
		}catch (Exception e)
	    {
		throw new IOException ("Caught exception processing input row",e);
	    }
	}

}
