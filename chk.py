import tlsh

class HashKey :
    loguB = 1
    m = 8

    def __init__(self, hexs=None) :
        if hexs is None :
            self.ks = tuple([0 for _ in range(HashKey.m)]) 
        else :
            self.ks = tuple([int(hexs[i*HashKey.loguB:(i+1)*HashKey.loguB],16) for i in range(HashKey.m)]) 

    def compute_l(self, other) :
        #how many characters in common?
        m = len(self.ks)
        assert(m == len(other.ks))
        for i,(e1,e2) in enumerate(zip(self.ks,other.ks)) :
            if e1 != e2 :
                return i
        return m #self == other
    
    def __cmp__(self, other) :
        l = self.compute_l(other)
        if l == len(self.ks) :
            return 0
        if self.ks[l] < other.ks[l] :
            return -1
        elif self.ks[l] == other.ks[l] :
            return 0
        else :
            return +1
    
    def __repr__(self):
        return str(self.ks)
        
    def __lt__(self, other):
        return self.__cmp__(other) < 0

    def __int__(self) :
        res = 0
        for v in self.ks :
            res <<= 4 * HashKey.loguB #4 bits per hex digit
            res |= v
        return res

    def distance(self, other) :
        l = 0
        while l<self.m and self.ks[l] == other.ks[l] :
            l += 1
        if l==self.m :
            return 0
        assert(self.ks[l] != other.ks[l]) 
        alpha_size = 16 ** HashKey.loguB
        kd = abs(self.ks[l] - other.ks[l])
        assert(kd <= alpha_size)
        kd /= alpha_size
        _dist = self.m - l - 1 + kd
        #print("debug", _dist, self.m, l, kd)
        assert(_dist <= self.m)
        return _dist
        
    
class CompoundHashKey :
    L = 8

    def __init__(self, hexs=None) :
        ks_len = HashKey.loguB*HashKey.m
        if hexs is None or len(hexs)==0 :
            self.kss = [HashKey(None) for _ in range(CompoundHashKey.L)]
        else :
            assert(len(hexs) >= HashKey.loguB * HashKey.m * CompoundHashKey.L)
            self.kss = [HashKey(hexs[i*ks_len:(i+1)*ks_len]) for i in range(CompoundHashKey.L)]
            self.kss.sort()
            #self.kss = [tuple([b[(j*m+i)*loguB:(j*m+i+1)*loguB] for i in range(m)]) for j in range(L)]     

    def __repr__(self):
        return str(self.kss)   
    
    def to_vector(self) :
        return [int(ks) for ks in self.kss]    
    
    def distance(self, other) :
        #print([ks1.distance(ks2) for ks1,ks2 in zip(self.kss,other.kss)])
        return min([ks1.distance(ks2) for ks1,ks2 in zip(self.kss,other.kss)])

 
if __name__ == '__main__' :
    data = [
        (0, "Antichi resti fossili di lupo furono ritrovati presso uno stanziamento umano in una tomba natufiana, e risalgono a 11 000-12 000 anni fa, ma l'origine del rapporto fra le due specie si colloca molto più indietro nel tempo, fra 30 000 e 36 000 anni fa.",),
        (1, "Antichi fossili di lupo furono ritrovati presso uno stanziamento umano in una tomba natufiana, e risalgono a 11 000-12 000 anni fa, ma l'origine del rapporto fra le due specie si colloca molto più indietro nel tempo, fra 30 000 e 36 000 anni fa."),
        (2, "Antichi resti di lupo furono ritrovati presso uno stanziamento umano in una tomba natufiana, e risalgono a 11 000-12 000 anni fa, ma l'origine del rapporto fra le due specie si colloca molto più indietro nel tempo, fra 30 000 e 36 000 anni fa."),
        (3, "Antichi lupo furono ritrovati presso uno stanziamento umano in una tomba natufiana, e risalgono a 11 000-12 000 anni fa, ma l'origine del rapporto fra le due specie si colloca molto più indietro nel tempo, fra 30 000 e 36 000 anni fa."),
        (4, "Antichi furono ritrovati presso uno stanziamento umano in una tomba natufiana, e risalgono a 11 000-12 000 anni fa, ma l'origine del rapporto fra le due specie si colloca molto più indietro nel tempo, fra 30 000 e 36 000 anni fa."),
        (5, "Antichi resti fossili di lupo furono ritrovati presso uno stanziamento umano in una tomba, e risalgono a 11 000-12 000 anni fa, ma l'origine del rapporto fra le due specie si colloca molto più indietro nel tempo, fra 30 000 e 36 000 anni fa."),
        (6, "Antichi resti fossili di lupo furono ritrovati presso uno stanziamento umano in una tomba, e risalgono a 11 000-12 000 anni fa, ma l'origine del rapporto fra le due specie si colloca molto più indietro nel tempo, fra 30 e 36 anni fa."),

        (7, "'तू जो बात नहीं समझती,उसमें टाँग क्यों अड़ाती है भाई! मेरी लाठी दे दे और अपना काम देख। यह इसी मिलते-जुलते रहने का परसाद है कि अब तक जान बची हुई है। नहीं कहीं पता न लगता कि किधर गये। गाँव में इतने आदमी तो है, किम पर बेदखली नहीं आयी, किस पर कुड़की नहीं पायी। जब दूसरे के पाँवों-तले अपनी गर्दन दबी हुई है,तो उन पाँवों को सहलाने में ही कुशल है।'"),
        (8, "'मेरी लाठी दे दे और अपना काम देख। यह इसी मिलते-जुलते रहने का परसाद है कि अब तक जान बची हुई है। नहीं कहीं पता न लगता कि किधर गये। गाँव में इतने आदमी तो है, किम पर बेदखली नहीं आयी, किस पर कुड़की नहीं पायी। जब दूसरे के पाँवों-तले अपनी गर्दन दबी हुई है,तो उन पाँवों को सहलाने में ही कुशल है।'"),
        (9, "'तू जो बात नहीं समझती,उसमें टाँग क्यों अड़ाती है भाई! यह इसी मिलते-जुलते रहने का परसाद है कि अब तक जान बची हुई है। नहीं कहीं पता न लगता कि किधर गये। गाँव में इतने आदमी तो है, किम पर बेदखली नहीं आयी, किस पर कुड़की नहीं पायी। जब दूसरे के पाँवों-तले अपनी गर्दन दबी हुई है,तो उन पाँवों को सहलाने में ही कुशल है।'"),
        (10, "'तू जो बात नहीं समझती,उसमें टाँग क्यों अड़ाती है भाई! मेरी लाठी दे दे और अपना काम देख। यह इसी मिलते-जुलते रहने का परसाद है कि अब तक जान बची हुई है। नहीं कहीं पता न लगता कि किधर गये। गाँव में इतने आदमी तो है, किम पर बेदखली नहीं आयी, किस पर कुड़की नहीं पायी। जब दूसरे के पाँवों-तले अपनी गर्दन दबी हुई है"),

        (11, "Ὁμώνυμα λέγεται ὧν ὄνομα μόνον κοινόν, ὁ δὲ κατὰ τοὔνομα λόγος τῆς οὐσίας ἕτερος, οἷον ζῷον ὅ τε ἄνθρωπος καὶ τὸ γεγραμμένον. τούτων γὰρ ὄνομα μόνον κοινόν, ὁ δὲ κατὰ τοὔνομα λόγος τῆς οὐσίας ἕτερος· ἂν γάρ τις ἀποδιδῷ τί ἐστιν αὐτῶν ἑκατέρῳ τὸ ζῴῳ εἶναι, ἴδιον ἑκατέρου λόγον ἀποδώσει. συνώνυμα δὲ λέγεται ὧν τό τε ὄνομα κοινὸν καὶ ὁ κατὰ τοὔνομα λόγος τῆς οὐσίας ὁ αὐτός, οἷον ζῷον ὅ τε ἄνθρωπος καὶ ὁ βοῦς. "),
        (12, "Ὁμώνυμα λέγεται ὧν ὄνομα μόνον κοινόν, ὁ δὲ κατὰ τοὔνομα λόγος τῆς οὐσίας ἕτερος. τούτων γὰρ ὄνομα μόνον κοινόν, ὁ δὲ κατὰ τοὔνομα λόγος τῆς οὐσίας ἕτερος· ἂν γάρ τις ἀποδιδῷ τί ἐστιν αὐτῶν ἑκατέρῳ τὸ ζῴῳ εἶναι, ἴδιον ἑκατέρου λόγον ἀποδώσει. συνώνυμα δὲ λέγεται ὧν τό τε ὄνομα κοινὸν καὶ ὁ κατὰ τοὔνομα λόγος τῆς οὐσίας ὁ αὐτός, οἷον ζῷον ὅ τε ἄνθρωπος καὶ ὁ βοῦς. "),
        (13, "Ὁμώνυμα λέγεται ὧν ὄνομα μόνον κοινόν, οἷον ζῷον ὅ τε ἄνθρωπος καὶ τὸ γεγραμμένον. τούτων γὰρ ὄνομα μόνον κοινόν, ὁ δὲ κατὰ τοὔνομα λόγος τῆς οὐσίας ἕτερος· ἂν γάρ τις ἀποδιδῷ τί ἐστιν αὐτῶν ἑκατέρῳ τὸ ζῴῳ εἶναι, ἴδιον ἑκατέρου λόγον ἀποδώσει. συνώνυμα δὲ λέγεται ὧν τό τε ὄνομα κοινὸν καὶ ὁ κατὰ τοὔνομα λόγος τῆς οὐσίας ὁ αὐτός, οἷον ζῷον ὅ τε ἄνθρωπος καὶ ὁ βοῦς. "),
        (14, "Ὁμώνυμα λέγεται ὧν ὄνομα μόνον κοινόν, ὁ δὲ κατὰ τοὔνομα λόγος τῆς οὐσίας ἕτερος, οἷον ζῷον ὅ τε ἄνθρωπος καὶ τὸ γεγραμμένον. τούτων γὰρ ὄνομα μόνον κοινόν, ὁ δὲ κατὰ τοὔνομα λόγος τῆς οὐσίας ἕτερος· ἂν γάρ τις ἀποδιδῷ τί ἐστιν αὐτῶν ἑκατέρῳ τὸ ζῴῳ εἶναι, ἴδιον ἑκατέρου λόγον ἀποδώσει. συνώνυμα δὲ λέγεται ὧν τό τε ὄνομα κοινὸν καὶ ὁ κατὰ τοὔνομα λόγος τῆς οὐσίας ὁ αὐτός. "),

        (16, "Сокр. В чём состоит? Донос-то, мне кажется, не C. маловажен: молодому человеку знать такое дело — не безделица. Я знаю, говорит он, каким образом развращается юношество, и знаю, кто развращает его."),
        (17, "Сокр. В чём состоит? Донос-то, не C. маловажен: молодому человеку знать такое дело — не безделица. Я знаю, говорит он, каким образом развращается юношество, и знаю, кто развращает его."),
        (18, "Сокр. В чём состоит? Донос-то, мне кажется: молодому человеку знать такое дело — не безделица. Я знаю, говорит он, каким образом развращается юношество, и знаю, кто развращает его."),
    
        (19, "Antichi resti fossili di lupo furono ritrovati presso uno stanziamento umano in una tomba natufiana, e risalgono a 11 000-12 000 anni fa, ma l'origine del rapporto fra le due specie si colloca molto più indietro nel tempo, fra 30 000 e 36 000 anni fa."),
    ]
    data_enc = [(i,d.encode('utf-8')) for i,d in data]
    data_tlsh = [(i,tlsh.hash(de)[8:]) for i,de in data_enc] #skip first 2 chars cause they are always 'T1' & 6 chars which are global according to https://github.com/idealista/tlsh/blob/master/README.md
    chks = [(i,CompoundHashKey(hexs)) for i,hexs in data_tlsh]

    for i,chk in chks :
        #print('Distances with',i)
        for j,chk2 in chks :
            print(chk.distance(chk2), end=',')
        print()

    for i,chk in chks :
        for ks in chk.kss :
            pass #print(i, int(ks))



    
