
var COOKIES = COOKIES || {
	
	cookie : 'traccovid.cookies',
	version : '2021-04-20',
	link : 'https://traccovid.com/traccovid/assets/terms.html#cookie',
	
	save : function (name, value, days) {
	    var expires = "";
	    if (days) {
	        var date = new Date();
	        date.setTime(date.getTime() + (days*24*60*60*1000));
	        expires = "; expires=" + date.toUTCString();
	    }
	    document.cookie = name + "=" + encodeURIComponent(value) + expires + "; path=/";
	},

	read : function read(name) {
	    var nameEQ = name + "=";
	    var ca = document.cookie.split(';');
	    for(var i=0;i < ca.length;i++) {
	        var c = ca[i];
	        while (c.charAt(0)==' ') c = c.substring(1,c.length);
	        if (c.indexOf(nameEQ) == 0) return decodeURIComponent( c.substring(nameEQ.length,c.length) );
	    }
	    return null;
	},

	erase : function (name) {
	    COOKIES.create(name,"",-1);
	},
	
	show_message : function () {
		if (!document.body || document.body == null) {
			setTimeout(COOKIES.show_message, 100);
		} else {
			var ver = COOKIES.read(COOKIES.cookie);
			if (!ver || ver == null || ver != COOKIES.version) {
				var html = '<button'
					+ ' class="btn btn-sm btn-primary"'
					+ ' style="float:right; width:128px;"'
					+ ' onclick="COOKIES.dismiss_message();"'
					+ '>OK</button>'
					+ '<div style="padding-top: 6px;">'
					+ ' We use cookies for the functionality of our tools and for analytics. '
					+ ' <a href="'+COOKIES.link+'" style="color: #ffffff; text-decoration: underline ! important;">'
					+ ' Please read our policies to find out more</a>.'
					+ '<br/></div>'
					+ '<div style="clear:both;"></div>'
					;
				
				var style = 'border: none; padding: 8px; margin: 0px; '
					+ 'font-family: sans-serif; font-size: 10pt; text-decoration: none; text-align: left; line-height: 1.2em; '
					+ 'background: #333333; color: #ffffff;';
				
				var elem = null;
				try {
					elem = document.createElement('<div style="' + style + '">');
				} catch (e) {}
				if (!elem) {
					elem = document.createElement('div');
				}
				
				elem.id = 'cookies_message';
				elem.setAttribute('style', style);
				elem.innerHTML = html;
				
				document.body.insertBefore(elem, document.body.firstChild);
			}
		}
	},
	
	dismiss_message : function () {
		document.getElementById('cookies_message').outerHTML = '';
		COOKIES.save(COOKIES.cookie, COOKIES.version, 365);
	}
};

COOKIES.show_message();