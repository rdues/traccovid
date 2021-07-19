
_expand_btn = function() {
	var btn = document.getElementById('btn-expand');
	if (btn != null) {		
		var div_ids = [
			'types-list',
			'keywords-list',
			'hashtags-list',
			'websites-list'
		];
				
		btn.onclick = function() {
			for (var i = 0; i < div_ids.length; i++) {
				var div = document.getElementById( div_ids[i] );
				if (div) {
					div.style.height = '';
				}
			}
		};
	} else {
		setTimeout(_expand_btn, 100);
	}
};
_expand_btn();

