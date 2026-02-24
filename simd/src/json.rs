use std::io::{self, Write};

pub fn write_escaped_json_bytes<W: Write>(writer: &mut W, data: &[u8]) -> io::Result<()> {
    let mut beg = data.as_ptr().cast_mut();
    let end = unsafe { beg.add(data.len()) };

    loop {
        let found = find_next_escape_raw(beg, end);
        let rel = unsafe { found.offset_from_unsigned(beg) };

        writer.write_all(unsafe { std::slice::from_raw_parts(beg, rel) })?;

        if found == end {
            return Ok(());
        }

        match unsafe { *found } {
            b'"' => writer.write_all(br#"\""#)?,
            b'\\' => writer.write_all(br"\\")?,
            b'\n' => writer.write_all(br"\n")?,
            b'\r' => writer.write_all(br"\r")?,
            b'\t' => writer.write_all(br"\t")?,
            b => writer.write_all(&[
                b'\\',
                b'u',
                b'0',
                b'0',
                b"0123456789abcdef"[(b >> 4) as usize],
                b"0123456789abcdef"[(b & 0x0F) as usize],
            ])?,
        }

        beg = unsafe { found.cast_mut().add(1) };
    }
}

fn find_next_escape_raw(beg: *mut u8, end: *mut u8) -> *const u8 {
    unsafe {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        return FIND_NEXT_ESCAPE_DISPATCH(beg, end);
        #[cfg(target_arch = "aarch64")]
        return find_next_escape_neon(beg, end);
        #[allow(unreachable_code)]
        find_next_escape_fallback(beg, end)
    }
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn find_next_escape_neon(beg: *mut u8, end: *mut u8) -> *const u8 {
    use std::arch::aarch64::{
        vandq_u8, vceqq_u8, vcgtq_u8, vcltq_u8, vdupq_n_u8, vget_high_u8, vget_lane_u16,
        vget_low_u8, vld1q_u8, vmaxvq_u8, vorrq_u8, vpadd_u8, vreinterpret_u16_u8,
    };

    const BIT_MASK: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];

    let mut base = beg;
    let mut remaining = unsafe { end.offset_from_unsigned(base) };

    let v_lo = vdupq_n_u8(0x20);
    let v_hi = vdupq_n_u8(0x7e);
    let v_dq = vdupq_n_u8(b'"');
    let v_bs = vdupq_n_u8(b'\\');
    // Safety: BIT_MASK is a 16-byte array with static lifetime.
    let v_bits = unsafe { vld1q_u8(BIT_MASK.as_ptr()) };

    while remaining >= 16 {
        let v = unsafe { vld1q_u8(base) };

        let lo = vcltq_u8(v, v_lo);
        let hi = vcgtq_u8(v, v_hi);
        let dq = vceqq_u8(v, v_dq);
        let bs = vceqq_u8(v, v_bs);
        let any = vorrq_u8(vorrq_u8(lo, hi), vorrq_u8(dq, bs));

        if vmaxvq_u8(any) != 0 {
            let masked = vandq_u8(any, v_bits);
            let sum = vpadd_u8(vget_low_u8(masked), vget_high_u8(masked));
            let sum = vpadd_u8(sum, sum);
            let sum = vpadd_u8(sum, sum);
            let mask = vget_lane_u16::<0>(vreinterpret_u16_u8(sum));
            // vmaxvq_u8 != 0 guarantees at least one lane is 0xFF, so mask != 0
            // and trailing_zeros() is in [0, 15].
            return unsafe { base.add(mask.trailing_zeros() as usize) };
        }

        base = unsafe { base.add(16) };
        remaining -= 16;
    }

    unsafe { find_next_escape_fallback(base, end) }
}

// In order to make `find_next_escape_raw` slim and fast, we use a function pointer that updates
// itself to the correct implementation on the first call. This reduces binary size.
// It would also reduce branches if we had >2 implementations (a jump still needs to be predicted).
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
static mut FIND_NEXT_ESCAPE_DISPATCH: unsafe fn(*mut u8, *mut u8) -> *const u8 =
    find_next_escape_dispatch;

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
unsafe fn find_next_escape_dispatch(beg: *mut u8, end: *mut u8) -> *const u8 {
    let func = if is_x86_feature_detected!("avx512bw") {
        find_next_escape_avx512bw
    } else if is_x86_feature_detected!("avx2") {
        find_next_escape_avx2
    } else {
        find_next_escape_fallback
    };
    unsafe {
        FIND_NEXT_ESCAPE_DISPATCH = func;
        func(beg, end)
    }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx512bw,avx512f")]
unsafe fn find_next_escape_avx512bw(mut beg: *mut u8, end: *mut u8) -> *const u8 {
    use std::arch::x86_64::{
        _mm512_cmpeq_epi8_mask, _mm512_cmpgt_epi8_mask, _mm512_loadu_epi8, _mm512_set1_epi8,
    };

    let v_lo = _mm512_set1_epi8(0x20);
    let v_hi = _mm512_set1_epi8(0x7e);
    let v_dq = _mm512_set1_epi8(b'"'.cast_signed());
    let v_bs = _mm512_set1_epi8(b'\\'.cast_signed());

    let mut remaining = unsafe { end.offset_from_unsigned(beg) };

    while remaining >= 64 {
        let v = unsafe { _mm512_loadu_epi8(beg.cast()) };

        // Same signed comparison trick as AVX2: cmpgt(v_lo, v) catches [0x00, 0x1F] ∪ [0x80, 0xFF].
        let lo = _mm512_cmpgt_epi8_mask(v_lo, v);
        // cmpgt(v, v_hi) catches [0x7F] only; [0x80, 0xFF] are negative in i8 and covered by lo.
        let hi = _mm512_cmpgt_epi8_mask(v, v_hi);
        let dq = _mm512_cmpeq_epi8_mask(v, v_dq);
        let bs = _mm512_cmpeq_epi8_mask(v, v_bs);

        // __mmask64 is u64; OR directly without a movemask step.
        let mask = lo | hi | dq | bs;
        if mask != 0 {
            return unsafe { beg.add(mask.trailing_zeros() as usize) };
        }

        beg = unsafe { beg.add(64) };
        remaining -= 64;
    }

    unsafe { find_next_escape_fallback(beg, end) }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn find_next_escape_avx2(mut beg: *mut u8, end: *mut u8) -> *const u8 {
    use std::arch::x86_64::{
        _mm256_cmpeq_epi8, _mm256_cmpgt_epi8, _mm256_loadu_si256, _mm256_movemask_epi8,
        _mm256_or_si256, _mm256_set1_epi8,
    };

    let v_lo = _mm256_set1_epi8(0x20);
    let v_hi = _mm256_set1_epi8(0x7e);
    let v_dq = _mm256_set1_epi8(b'"'.cast_signed());
    let v_bs = _mm256_set1_epi8(b'\\'.cast_signed());

    let mut remaining = unsafe { end.offset_from_unsigned(beg) };

    while remaining >= 32 {
        let v = unsafe { _mm256_loadu_si256(beg.cast()) };

        // cmpgt(v_lo, v): signed 32 > v catches [0x00, 0x1F] (control chars) and
        // [0x80, 0xFF] (negative in i8) in a single comparison, requiring no separate mask.
        let lo = _mm256_cmpgt_epi8(v_lo, v);
        // cmpgt(v, v_hi): signed v > 126 catches only [0x7F]; [0x80, 0xFF] are negative
        // in i8 so they do not satisfy v > 126 and are already covered by lo.
        let hi = _mm256_cmpgt_epi8(v, v_hi);
        let dq = _mm256_cmpeq_epi8(v, v_dq);
        let bs = _mm256_cmpeq_epi8(v, v_bs);

        let any = _mm256_or_si256(_mm256_or_si256(lo, hi), _mm256_or_si256(dq, bs));

        // movemask extracts bit 7 of each byte-lane into a 32-bit integer; bit i
        // corresponds to lane i, so trailing_zeros gives the index of the first escape.
        let mask = _mm256_movemask_epi8(any);
        if mask != 0 {
            return unsafe { beg.add(mask.trailing_zeros() as usize) };
        }

        beg = unsafe { beg.add(32) };
        remaining -= 32;
    }

    unsafe { find_next_escape_fallback(beg, end) }
}

unsafe fn find_next_escape_fallback(beg: *mut u8, end: *mut u8) -> *const u8 {
    unsafe {
        let mut base = beg;
        let mut remaining = end.offset_from_unsigned(base);

        while remaining >= 8 {
            let chunk = (base as *const u64).read_unaligned();

            // XOR constants 0x02 and 0x5C both have bit 7 = 0, so !xor_variant & 0x8080... ==
            // !chunk & 0x8080..., allowing a single shared mask across lo_q and bs.
            let not_hi = !chunk & 0x8080_8080_8080_8080;

            // lo_q detects [0x00, 0x1F] ∪ {0x22}: XOR shifts 0x22 -> 0x20, placing it just
            // below the 0x21 subtraction threshold alongside [0x00, 0x1F]; 0x20 shifts to
            // 0x22, which does not underflow. A borrow from byte i produces a false positive
            // only at byte i+1 (higher position), so trailing_zeros returns the true hit first.
            let lo_q = (chunk ^ 0x0202_0202_0202_0202).wrapping_sub(0x2121_2121_2121_2121) & not_hi;

            // bs detects {0x5C} via the has-zero-byte identity after XOR; 0x5C has bit 7 = 0,
            // so not_hi suppresses false positives from high bytes without a separate mask.
            let bs = (chunk ^ 0x5C5C_5C5C_5C5C_5C5C).wrapping_sub(0x0101_0101_0101_0101) & not_hi;

            // hi_del detects [0x7F, 0xFF]: adding 1 to 0x7F overflows into bit 7, and
            // [0x80, 0xFF] already have bit 7 set. A carry from 0xFF produces a false positive
            // only at the next byte (higher position), so trailing_zeros is unaffected.
            let hi_del =
                (chunk.wrapping_add(0x0101_0101_0101_0101) | chunk) & 0x8080_8080_8080_8080;

            let any = lo_q | bs | hi_del;

            if any != 0 {
                let idx = (any.trailing_zeros() / 8) as usize;
                return base.add(idx);
            }

            base = base.add(8);
            remaining -= 8;
        }

        if remaining >= 4 {
            let chunk = (base as *const u32).read_unaligned();

            let not_hi = !chunk & 0x8080_8080;
            let lo_q = (chunk ^ 0x0202_0202).wrapping_sub(0x2121_2121) & not_hi;
            let bs = (chunk ^ 0x5C5C_5C5C).wrapping_sub(0x0101_0101) & not_hi;
            let hi_del = (chunk.wrapping_add(0x0101_0101) | chunk) & 0x8080_8080;
            let any = lo_q | bs | hi_del;

            if any != 0 {
                let idx = (any.trailing_zeros() / 8) as usize;
                return base.add(idx);
            }

            base = base.add(4);
            remaining -= 4;
        }

        if remaining >= 2 {
            let chunk = (base as *const u16).read_unaligned();

            let not_hi = !chunk & 0x8080;
            let lo_q = (chunk ^ 0x0202).wrapping_sub(0x2121) & not_hi;
            let bs = (chunk ^ 0x5C5C).wrapping_sub(0x0101) & not_hi;
            let hi_del = (chunk.wrapping_add(0x0101) | chunk) & 0x8080;
            let any = lo_q | bs | hi_del;

            if any != 0 {
                let idx = (any.trailing_zeros() / 8) as usize;
                return base.add(idx);
            }

            base = base.add(2);
            remaining -= 2;
        }

        if remaining >= 1 {
            let b = *base;
            if !(0x20..=0x7E).contains(&b) || b == b'"' || b == b'\\' {
                return base;
            }
        }

        end
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probe(data: &mut [u8]) -> Option<usize> {
        let beg = data.as_mut_ptr();
        let end = unsafe { beg.add(data.len()) };
        let found = find_next_escape_raw(beg, end);
        if found == end { None } else { Some(unsafe { found.offset_from_unsigned(beg) }) }
    }

    #[test]
    fn safe_chunk_returns_none() {
        let mut data = *b"ABCDEFGH";
        assert_eq!(probe(&mut data), None);
    }

    #[test]
    fn control_char_at_byte_zero() {
        let mut data = [0x01, b'A', b'A', b'A', b'A', b'A', b'A', b'A'];
        assert_eq!(probe(&mut data), Some(0));
    }

    #[test]
    fn quote_only_chunk() {
        let mut data = [b'"'; 8];
        assert_eq!(probe(&mut data), Some(0));
    }

    #[test]
    fn quote_at_interior_index() {
        let mut data = *b"AAA\"AAAA";
        assert_eq!(probe(&mut data), Some(3));
    }

    #[test]
    fn backslash_only_chunk() {
        let mut data = [b'\\'; 8];
        assert_eq!(probe(&mut data), Some(0));
    }

    #[test]
    fn backslash_at_interior_index() {
        let mut data = *b"AAAAA\\AA";
        assert_eq!(probe(&mut data), Some(5));
    }

    #[test]
    fn del_only_chunk() {
        let mut data = [0x7F; 8];
        assert_eq!(probe(&mut data), Some(0));
    }

    #[test]
    fn tail_7_escape_at_index_4() {
        let mut data = [b'A', b'A', b'A', b'A', 0x01, b'A', b'A'];
        assert_eq!(probe(&mut data), Some(4));
    }

    #[test]
    fn tail_6_escape_at_index_5() {
        let mut data = [b'A', b'A', b'A', b'A', b'A', 0x01];
        assert_eq!(probe(&mut data), Some(5));
    }

    #[test]
    fn tail_3_escape_at_index_2() {
        let mut data = [b'A', b'A', 0x01];
        assert_eq!(probe(&mut data), Some(2));
    }

    #[test]
    fn offset_escape_in_second_chunk_start() {
        let mut data = [b'A'; 16];
        data[8] = 0x01;
        assert_eq!(probe(&mut data), Some(8));
    }

    #[test]
    fn offset_escape_in_second_chunk_interior() {
        let mut data = [b'A'; 16];
        data[11] = 0x01;
        assert_eq!(probe(&mut data), Some(11));
    }

    #[test]
    fn offset_escape_in_single_byte_tail() {
        let mut data = [b'A'; 17];
        data[16] = 0x01;
        assert_eq!(probe(&mut data), Some(16));
    }

    #[test]
    fn high_byte_chunk_returns_zero() {
        let mut data = [0x80; 8];
        assert_eq!(probe(&mut data), Some(0));
    }

    #[test]
    fn high_byte_at_interior_index() {
        let mut data = *b"AAAA\x80AAA";
        assert_eq!(probe(&mut data), Some(4));
    }

    #[test]
    fn boundary_0x7e_is_safe() {
        // 0x7E is the last safe non-special ASCII byte; the scanner must not trigger.
        let mut data = [0x7E; 8];
        assert_eq!(probe(&mut data), None);
    }

    #[test]
    fn boundary_0x7f_is_escape() {
        let mut data = [0x7F; 8];
        assert_eq!(probe(&mut data), Some(0));
    }
}
