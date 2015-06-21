import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

@Description(
		name ="format_genre",
		value = "returns the format we want for the movie genre",
		extended = "select format_genre(genres) from movie;"
		)


public class format_genreh extends UDF
{
	public Text evaluate(Text input)
	{
		if(input == null)
		{
			return null;
		}
		
			String str = input.toString();
			String out = "";
			if(str.contains("|"))
			{
				String [] s = str.split("\\|");
				for(int i =0; i<s.length;i++)
				{
					if(i==(s.length-1))
					{
						out+=("&"+(i+1)+")"+s[i]+" yxc135530:hive");
					}
					else
					{
						out+=((i+1)+")"+s[i]+",");
					}
				}
				return new Text(out);
			}
			out = "1)"+str+" yxc135530:hive";
			return new Text(out);
	  }

}
