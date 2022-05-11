package net.virtualvoid.restic

import java.nio.ByteBuffer
import javax.crypto.Cipher
import javax.crypto.spec.{ IvParameterSpec, SecretKeySpec }

class Decryptor(keySpec: SecretKeySpec) {
  val cipher = Cipher.getInstance("AES/CTR/NoPadding")
  val ivBuffer = new Array[Byte](16)

  def decrypt(in: ByteBuffer): Array[Byte] = {
    in.get(ivBuffer, 0, 16)
      .limit(in.limit() - 16)
    val ivSpec = new IvParameterSpec(ivBuffer)
    cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec)
    val outputBuffer = ByteBuffer.allocate(in.remaining())
    cipher.doFinal(in, outputBuffer)
    outputBuffer.array()
  }
}