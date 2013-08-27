package hstore

var encDecTests = []struct {
	in  map[string]string
	out string
}{
	{
		map[string]string{},
		"",
	},
	{
		map[string]string{"k1": "v1"},
		`k1=>v1`,
	},
	{
		map[string]string{"k1": "v1", "k2": "v2"},
		`k1=>v1, k2=>v2`,
	},
	{
		map[string]string{"k1": "v1", "k2": "v2", "k3": "v3"},
		`k1=>v1, k2=>v2, k3=>v3`,
	},
	{
		map[string]string{"k 1": "v1", "k2": "v 2", "k 3": "v 3"},
		`"k 1"=>v1, k2=>"v 2", "k 3"=>"v 3"`,
	},
	{
		map[string]string{"k,1": "v1", "k2": "v,2", "k,3": "v,3"},
		`"k,1"=>v1, k2=>"v,2", "k,3"=>"v,3"`,
	},
	{
		map[string]string{"k>1": "v1", "k2": "v>2", "k>3": "v>3"},
		`"k>1"=>v1, k2=>"v>2", "k>3"=>"v>3"`,
	},
	{
		map[string]string{"k=1": "v1", "k2": "v=2", "k=3": "v=3"},
		`"k=1"=>v1, k2=>"v=2", "k=3"=>"v=3"`,
	},
	{
		map[string]string{"k=>1": "v1", "k2": "v=>2", "k=>3": "v=>3"},
		`"k=>1"=>v1, k2=>"v=>2", "k=>3"=>"v=>3"`,
	},
}
