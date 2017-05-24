using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace M3u8Parser.Utils
{
    public static class EscapeTablekey
    {
        public static string Replace(string key)
        {
            Regex DisallowedCharsInTableKeys = new Regex(@"[\\\\#%+/?\u0000-\u001F\u007F-\u009F]");
            string sanitizedKey = DisallowedCharsInTableKeys.Replace(key, "_");
            return sanitizedKey;
        }
    }
}
