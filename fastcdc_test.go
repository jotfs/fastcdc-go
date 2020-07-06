package fastcdc

import (
	"bytes"
	"encoding/hex"
	"io"
	"log"
	"math/rand"
	"testing"
)

var defaultOpts = Options{
	AverageSize: 1024,
	Seed:        84372,
}

var defaultData = randBytes(4321, 7291)

// Chunks generated with defaultOpts from defaultData
var defaultChunks []Chunk = []Chunk{
	{
		Offset:      0,
		Length:      520,
		Fingerprint: 1178648815341096960,
		Data:        decodeHex("6d976aad70991b55770a5287717f55f4dd892e0e43f330a8d442043aff8ed2844a9115e84e7389ef3d8332171895f6aa21a162f3bd1cb7b956ebb7dcdf24a0c1abd1dac7c6f370a608208b0e589fde1645f8bedbb3a78e1213572e5e14f35373fa8b015cc6f57747921f464b48b1d4edabab53c6c56a2ed606f069487d187ef50b31b712c5b04570adf99c1adf6100ea383fbe8e04008e900a1e2344cd5f050ebbe3bed8f758375e2c7d5df82d7289029f0844d3b2bdb75ee158ffb5da270c00be27f69a6be5c6a4ccdf2c693ca4ec1be7b17b7889ff57275025178470d8544e9bbb547ce174f77e7f97e66278c19701b828b63c0612475d8427bd631d67b80da3b14f3b8d392bb40ea5f9107a53a60fb45ac176b3bf72044eb98d091be75f3433fa614baad7f5b512704f83741cd6277084e6c1ab9c5b107634d55d27ebd07819d87874ba1a3b66909fd405b9c0757589dd65c1a43da814f9fa242f22fb8a70453a297a591ceb9bdad65f760c734d82385f175de1476afeda3890211c2012be528f05c7a2d776304a9ac218b2623a01f7efa45c90605f50a4178abfdab21011f93c317cd711923b4fe482f714b179e574dbd2b56d0660e925e458549566264143a41a8576ab2860ea665f347301f2e83d63321770459c4322ace88bd2c16a6df4fbe9636950e2ed57c25ac0717ffe7dc5d3174cebaff21ad89137ac297b547905a841ebe28f85a2"),
	},
	{
		Offset:      520,
		Length:      1412,
		Fingerprint: 17216826574435771136,
		Data:        decodeHex("d35a4adcc90b5df03e903d88dcf510785c577e76ca3aa0711499bf21cf3fc1731843274f08858f130e819c8121d03a6222fb73f2c24d71c254c9d98f7d4e6fa8e1ca7f9ded68517d4ca8aed415151dca8ab4b440eb4e0e8a4155d7ef8882e9f9efa47ee889a19c135b16dbf7cb3cba3f8f89e3dbf05edcb70cce712ccfdccc63b5d0cdbf8a2a84e98b5ca64b39e9a2e7e5f6a3eeb344066df2237afa439f7990317f152bb9d15cf1c790bbcbb9db5654bb104f1b3b9d21a811e2b7219879653e451cbac79f87f394de1c5af43bdcbc1915462f9fdaaa0e05a8af6c8e7d125a7074b2f9e29916fd21fc7a593d795691d46fa818f56117e4c44f6c7bf846cade0ee6cc81a52cfa1b14964b5de581d541e982139183089080cda0dd2c9f70935c5a1698370a88ca39235195cfb40ca4f4e0a68e584f3e2d7cbb5380e4a0d83a40daf1ad5438acc863d3d299eb96eaa3aff01ea61fea65dfc259efc22187c0d31b456c807dab33b733cb5e355397cedc1c3fe2f75315bdaee1165b535a45b879e8641860141c8a1ef0fa7675a889d2241dd56efd5291fefe9cb3de0152ab7893c548d831d847679b871ce4c8c23c9eee8718fbb910516af62b068a25b2b96481cad924fdcf401afb4e9a776d0e771441c3d37367da2f2998530ab8258eb6fa6501a099cf2ff0566934ca985f911f1cf1e9bbf517ff323e1e652f55a82fc081b99a66e5bc1744d9be9d9909e32063ba5ea4d9363bd7093dfbf41f59e450b48ec8f430d68bdcde149af66a933c530c176686c89442f4a744c060c735aa30b119825115656154d1d62d8495498d655ef6fed18d44694241105a9fa4a398bc02da4c70d96722ae1fd71f9c1cfc4bedbc4fe30d2399d4c3eb4e8328e83fdbbaadbcef3ee25b6390b53320796134a529d9bf0994be63a909e2c0d6fbee250faf34f126d1aa169fc1b28d9847e25f0e6701c367161934fb5702357b5f4ebda5740ab7f0ec8fe12f01e206aebaf9772216a4fcb2a8a9a117550f59ced4fedea7fa1753a7b09dbc87e6d234d311ae896f2eadb74258c137d07ce83581ccd23b00f1cc9f247289feb3a520846e26b4c613354c5801126a8a598b3b31b02646c81cf05e2e1e4efd8532f3e803531572346f64285e71fd1aaf028dae6f00ab70b3d789c42694ca691a754e9da14c91af3be1f9730e1872d5a3db959ed0a351742239c86816743b6558d9c0fcee8ee9be93770533ede7bb3f2c9ca60d38075c9f2968c939f9d9fd1072126d7ac4beccb7c524030d005aff270e6b23e6906c90933bae30dd4df5de6cc9c31e27af8c933f6602986023558d02ef5d3f9c33281688614c1b361c41daff9cc87abee846fe82ffbd0ea25b577b3ec8504387c0a9f9660f595f1c0d8d349e3330090f20fab566d1dc19188734ee7c0a1d769a9afaba82083450bd9631414e5cab2e219391f6acd1b723a2e7751ee073f01c33c2b37c9f3764c9faf66d72198d523a921811fe31584c8c407645fb0a4fb0b5e4e565e3d95f11a4e33154701335c060b93c0b0350685ac7d3525f376572252a2389a5c5517f036f3e6ea155da0295e5ed77cdc65e6de8a3d17f9d096f2cf40ec6fd5c0fc5f79fecf4ab4f173ca08f50e0aa3dbcc68a6d8c3e712fe2478d9fa5f994e50c71a3ddcc1eef36779c0738d119483a5441eeb312bc668ea31369ae231ede6294a3c9f1556ae256f0f386615340d1864d971437b42383477c738bec11a61a3c8c41cc468d212fd3d9b770f19309c58ed5c7652514d647c4ae351a703906bb5baa248e60c83c97fb8d819689a27b5810d7363714f7d3ab7d0dfad00299973291e4cf746d6f63a6b8ba5b5fd90a7c4003109a053579e887284288fdc0bb0c9f839ebe8bfcdf21c333346eb258b14db4cacf96b185dd1eb2a4e47493473bd1ea28a0e5af3dff8b531ec52f2824cc833bb510c071777e8b00a491944749cfcc9e2198469abd9c1286d9d6cd1a99a218"),
	},
	{
		Offset:      1932,
		Length:      344,
		Fingerprint: 10800677755204489216,
		Data:        decodeHex("af5d11e942df50a9fc90131ddfbacb47ac58bce40cf6317ea1ed0d67067cc95b10e5909c1f2eb402bd1334e60fb92bbafe66efed05ab66c7c931c15ead1f6bfb77ab205bac93b312024d6131449e44b7ace677d1ccd2e82e9f9bc2218e0e5d7e60e9bc78d32f2fa5e5cf7fef727229ef53ae5b96205c2ceb50068c95fc6f0ff58b493b4f7a68ec2785ecec59eab1433d39532a975d3dc2ee10b357d6528dbe9b1abea4832f4bcbaf6262b8a1d1890d95e0bb584b0d19d7dff74bee1c4e72673522d71d920efa89ff88545bd4047ab24deae15a23e1c327e365b47d6fee63bfc6e550b8544b66b2bd1874a0d5f97cda4912a9b416cdfd7968fc2151d1267a93b00ab7be2e4520a260b7064868b0c07ed2311c86a9a130a47e988afb083741a93e1350480717bfac8e8c25fd74528dbe0bcb934207bcdd4585a106794464b4b76d1ab2a7627b94cc04007c87b81e041d98699e1526e5cb3a95"),
	},
	{
		Offset:      2276,
		Length:      1374,
		Fingerprint: 5337421803424907776,
		Data:        decodeHex("b999cdb20a0eb74bd0c97c1fda54edfd03c11f396b313ec5808910eff8191b30d18f41c95a3d8d242fda727597f8a38d979160e099ce85b779b569b0dd5ca857ad5c59624ef127450836550f3c9c463ff73b597f2602bca89a4cd8287033311c54c16792cfb9ac1e0e87467ab869f2e7eaf2c47d9b7f46e6c9164cc0fbc0e54c111c20d34a8d4f7709fc85400fb80a7c31d4f008cd6c15e115aa25fe21d7ee878f45bc975083a1b331793434a54bc7dba295940fdd72641b3f3a033bbc678c911f634ab25366684b60ac5ad07313f03ff0d0a0b001bd31bc5f452a9a2dc395bed8597c4dfb15fc03b7b01736cd42cb0e81c27296891f165c144b9b93ded30d0872065a101272235a11daa2d04c50a8dda3567da989e9e26310c26551d84252dd71e3cb5cb5b8cf683dceb149ad7e5ecd72bc0752e43c6e262f470abc49cb93f3e5ebe417b643f3ef0f6c6dce404e830b5bcbdfeb893861a08ea3b10ea48f643192d6d071d3ef07f41738b9fcee269504761e5eb445a89f5d77325f399e286f2cbd2e541b11e388ee8cf980a2eec32b97184833cea206324cd0d13bbc93c9524928b58f278e2bd3d2492f31ea5d40e8dfa93f544c089bdbabda88884a52f2289767819c0d79394f0732a76d79b06e0f3ce6207511973b57b22ca78c678bc9b52cc4a54353f785e0241887104c2e24e6419be1466e439adc7d8d5cae78c965f46800fdb58d9b1a6cd42341c2e53c7c3b8326d5f86f9d6626cfdc07c385209713771bcce1befff4138d718c520fc48571aea0ff8a928de847a7977cb8996701b639a5e2c6b63cc89ffc553a5c918ff0d26bd8857f753ce699d48a93e9bdec6e8996205de3761ad6f62c16dbff17105578ebaa7ad6447e91582b3e4ec752cc38692d46ac0b60724d19aa06749983a8169d59829938925fac7440ff7c19bad7aa03f4730a881f62e4a76dff625a3d615332cc2a8d3f77a6fa41ded7faf6361d98f693d90b9a03b0a373dd06ad93e5cce050f4f19b5ef4ae52e442d11d016a1fc9a73bedacc241f6979702ff4460cb07ad49455da02cb1670b12fc4d59951719d67106b786bcb6aeafa8bfcab08b2633f1426f586cf0b07cffede5c4ca3f6c59c68a6d4a1378cf663b7f1914b6425113bf9d0ce53b238119c78e96acaaad65c2aa23318d426f3507b10c570f63d2d50a469a0dc167de5f3af93594d290f8935d0183f089b8861aef27343c2ef4fe4e7134ede0b507d949d3ca69928a1cce6d1137b168426005ba48c1adbac90ff941f5887546f60ec455f5ac8f23ca82283b98ca45dbf62c7bec14d15d280cec337c082f0dbce141e75dec24352cf245ca10393846d0318dcf01a7524fbaeb52429331d4a86b0e9509dd57cbf0cd2a7f20df27930c1acdb0db71169000e7fca7259c53c3ad0f5f51914abdc7231b345e450fcd120f17cd0305c186ecbe5d297f92f822ef5d4e89c015a6aadb53090d37e8dc5d50c5ab519a67a5a940554fa953a9adf67ef09d19692ed56dd108e49775fd0266c2968f9ee273c2cdf321f5364ffd1ce5a575b07676bfb8aa7c0c31b554e787ad1b430d3c0422931fe3bef9c173a759339ea554a679d03381e6f3cf4b2c56dd26377a930fcc18d6330793978a78d5f0c3e510c71affd40df9d56089346ddeb011387b084d9e7045cbf6cb821651133aa7258d791fddfaa71d632673c91126137b6fabd411ee0d6147a1d096588dd35cf36c4a6d85b132680f9b59b671f5b3bde2b70505bbb8932bff515bdec06d86446256a524c605f56580529a4be4103a027d30a43956c0b499e29c585778d3862a1c9bb713471325adf2b4ae5568e61ffc182bfc546e163341420e8abf739ae404089d00654288dd390152c376b31950fd75837225271577d73e36214195766a9233044063be60970229626dd366be2da4a8b2"),
	},
	{
		Offset:      3650,
		Length:      671,
		Fingerprint: 16455672957995988310,
		Data:        decodeHex("30877d4ce3a3155d242a27e73ccc0c822f7130f137c8c9cc9d5bf8e9194dc4ca95e069c0c537b69e9f4d327bc39883a342c15d90c362fd68ab7443f58eeb18dc4a6f1834662ddab5800bdb3d95c49697667c027050fc53e0928a0047ff8dac3b41407456560f98b22805cf9dea6dd458fe0611351c2d8a9f738a38ad6336a9f54f167afde26ccb8bfa4e83261e58c79a880ab4dfd1d69556fca8d9b317a445dd4b5a4fe7ec483d01f3726f7402303abca81df9f1761029edb87e0c52a3ed1852193772ae582a883865b2a014cdde703295f1e413e9149ea44b52d82e4795c676a1046519dab66342d5ca2c28d526b08a70b36d968e3517762b5936486982e251bba6325a812fdeb14cc76f3e7c3e98e8fbabd9e2e61fa17029c946811371351ed0b294ac8013a534aa25bbba3b3c547f70649a66a36b948bcbfe6879f389dcfcf6a564c47e7a1abd051860f6426db6f23f815faa29b484d3323772b616da282253a321f369e1961e1304db3e484cb4357a184f087281955450da9f55bcc502876991f8863918152fe02ebb6ca13a06852fb1ef6d627e0387056bc2f5b0fb7758ede4d494be94860398eb6d8af22898b33e08628f6822a710a7c1775da705f02688e7647940beeabe09381688cd80623fbbe6720451a785d9dd010e03cc10f84aa291e0760940e3ff901742060832725730947783c15671e75b732e99d3e5f0360ef5715113bc9f63d9c5069550297a43f512195e48440ebc8523a71c3ed7bd71ab0718d70a4b8b059d54b278962c6bced7cbb398c9ea65791fd4cbede0bfa26f06a9e2d64a7b455e0e9618e66f4c552507f5bacad9936415ef170acafb69469f4f5abdc68e919024c9254a9c7257aa90eb9e86d7b742b87bad69a9b60559a9eaa1409e5e0faf1414884de6829a52399cd181f32be59b81911eece3e6c6ba33"),
	},
}

func TestChunking(t *testing.T) {
	chunker, err := NewChunker(bytes.NewReader(defaultData), defaultOpts)
	assertNoError(t, err)

	for i := 0; ; i++ {
		c, err := chunker.Next()
		if err == io.EOF {
			break
		}
		assertNoError(t, err)
		cdef := defaultChunks[i]

		if c.Offset != cdef.Offset {
			t.Errorf("chunk %d: expected offset %d but received %d", i, cdef.Offset, c.Offset)
		}
		if c.Length != cdef.Length {
			t.Errorf("chunk %d: expected length %d but received %d", i, cdef.Length, c.Length)
		}
		if c.Fingerprint != cdef.Fingerprint {
			t.Errorf("chunk %d: expected fp %x but received %x", i, cdef.Fingerprint, c.Fingerprint)
		}
		if !bytes.Equal(cdef.Data, c.Data) {
			t.Errorf("chunk %d: data not equal to expected", i)
		}
	}
}

func TestChunkingRandom(t *testing.T) {
	data := randBytes(1e6, 63)
	chunker, err := NewChunker(bytes.NewReader(data), defaultOpts)
	assertNoError(t, err)

	var prevOffset int
	var prevLength int
	allData := make([]byte, 0)
	for i := 0; ; i++ {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		assertNoError(t, err)

		offset := prevOffset + prevLength
		if offset != chunk.Offset {
			t.Errorf("chunk %d: Offset should be %d not %d", i, offset, chunk.Offset)
		}
		if chunk.Length != len(chunk.Data) {
			t.Errorf("chunk %d: Length %d does not match len(Data) %d", i, chunk.Length, len(chunk.Data))
		}

		allData = append(allData, chunk.Data...)

		prevOffset = chunk.Offset
		prevLength = chunk.Length
	}
	if !bytes.Equal(allData, data) {
		t.Error("data does not match")
	}
}

func TestMinSize(t *testing.T) {
	// Test with data smaller than min chunk size
	data := randBytes(10, 51)
	opts := defaultOpts
	opts.DisableNormalization = true
	chunker, err := NewChunker(bytes.NewReader(data), opts)
	assertNoError(t, err)

	c, err := chunker.Next()
	assertNoError(t, err)
	if !bytes.Equal(data, c.Data) {
		t.Error("data not equal")
	}
	if c.Length != len(data) {
		t.Errorf("invalid length %d", c.Length)
	}

	_, err = chunker.Next()
	if err != io.EOF {
		t.Error("expected io.EOF error")
	}
}

func TestOptionsValidation(t *testing.T) {
	avg := 1024 * 64
	testOpts := []Options{
		// AverageSize not set
		{},
		// MinSize too small
		{AverageSize: avg, MinSize: 1},
		// MaxSize too big
		{AverageSize: avg, MaxSize: maxSize + 1},
		// MaxSize less than MinSize
		{AverageSize: avg, MaxSize: avg / 2, MinSize: avg * 2},
		// AverageSize less than MinSize
		{AverageSize: avg, MinSize: 2 * avg, MaxSize: 4 * avg},
		// Bad normalization
		{AverageSize: avg, Normalization: 100},
		// BufSize too small
		{AverageSize: avg, BufSize: 1},
	}
	var r bytes.Reader

	for i, opts := range testOpts {
		_, err := NewChunker(&r, opts)
		if err == nil {
			t.Fatalf("%d: expected error", i)
		}
	}
}

func BenchmarkFastCDC(b *testing.B) {
	// total of 10GiB of data to chunk
	n := 10
	benchData := randBytes(100*1024*1024, 345)
	r := newLoopReader(benchData, n)

	b.ResetTimer()
	cnkr, err := NewChunker(r, Options{
		AverageSize: 1 * miB,
	})
	if err != nil {
		b.Fatal(err)
	}
	for {
		_, err := cnkr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(n * len(benchData)))
}

type bencSpec struct {
	size int
	name string
}

var bSizes = []bencSpec{
	{   1 << 10,   "1k"},
	{   4 << 10,   "4k"},
	{  16 << 10,  "16k"},
	{  32 << 10,  "32k"},
	{  64 << 10,  "64k"},
	{ 128 << 10, "128k"},
	{ 256 << 10, "256k"},
	{ 512 << 10, "512k"},
	{   1 << 20,   "1M"},
	{   4 << 20,   "4M"},
	{  16 << 20,  "16M"},
	{  32 << 20,  "32M"},
	{  64 << 20,  "64M"},
	{ 128 << 20, "128M"},
	{ 512 << 20, "512M"},
	{   1 << 30,   "1G"},
}

func BenchmarkFastCDCSize(b *testing.B) {
	for _, s := range bSizes {
		s := s
		b.Run(s.name, func(b *testing.B) {
			benchmarkFastCDCSize(b, s.size)
		})
	}
}

func benchmarkFastCDCSize(b *testing.B, size int) {
	rng := rand.New(rand.NewSource(1))
	data := make([]byte, size)
	rng.Read(data)

	r := bytes.NewReader(data)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	cnkr, err := NewChunker(r, Options{
		AverageSize: 1 * miB,
	})
	if err != nil {
		b.Fatal(err)
	}

	var res uint64
	var nchks int64

	for i := 0; i < b.N; i++ {
		r.Reset(data)
		cnkr.Reset(r)

		for {
			chunk, err := cnkr.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				b.Fatal(err)
			}
			res = res + uint64(len(chunk.Data))
			nchks++
		}
	}
	b.ReportMetric(float64(nchks)/float64(b.N), "chunks")
}

// loopReader implements io.Reader, looping over a provided buffer a given number of
// times.
type loopReader struct {
	n    int
	data []byte
	r    *bytes.Reader
	i    int
}

func newLoopReader(data []byte, n int) *loopReader {
	return &loopReader{n, data, bytes.NewReader(data), 0}
}

func (lr *loopReader) Read(p []byte) (int, error) {
	n, err := lr.r.Read(p)
	if err == io.EOF && lr.i < lr.n {
		lr.i++
		lr.r.Reset(lr.data)
		return n, nil
	}
	return n, err
}

func assertNoError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("expected no error but received: %v", err)
	}
}

func decodeHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		log.Fatal(err)
	}
	return b
}

func randBytes(n int, seed int64) []byte {
	b := make([]byte, n)
	rnd := rand.New(rand.NewSource(seed))
	_, err := rnd.Read(b)
	if err != nil {
		log.Fatal(err)
	}
	return b
}
