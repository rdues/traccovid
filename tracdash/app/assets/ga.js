_gafn_count = 0
_gafn = function() {
	try {
		window.dataLayer = window.dataLayer || [];
		function gtag(){dataLayer.push(arguments);}
		gtag('js', new Date());
		gtag('config', 'G-NL85KPYYCS');
	} catch(e) {
		_gafn_count++;
		if (_gafn_count < 50) {
			setTimeout(_gafn, 100);
		}
	}
};
_gafn();
